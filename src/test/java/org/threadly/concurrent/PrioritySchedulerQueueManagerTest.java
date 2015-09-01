package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.QueueManager;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSet;
import org.threadly.concurrent.AbstractPriorityScheduler.TaskWrapper;
import org.threadly.concurrent.PriorityScheduler.WorkerPool;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerQueueManagerTest {
  private static final String THREAD_NAME = "fooThread";
  
  private WorkerPool workerPool;
  private QueueManager queueManager;
  
  @Before
  public void setup() {
    ConfigurableThreadFactory threadFactory = new ConfigurableThreadFactory();
    workerPool = new WorkerPool(threadFactory, 1);
    queueManager = new QueueManager(workerPool, THREAD_NAME, 
                                    AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS) {
      @Override
      protected void startupService() {
        // we override this so we can avoid starting threads in these tests
        runningThread = Thread.currentThread();
      }

      @Override
      protected void shutdownService() {
        // override since the service was never started
        runningThread = null;
      }
    };
  }
  
  @After
  public void cleanup() {
    workerPool.startShutdown();
    workerPool.finishShutdown();
    queueManager.stopIfRunning();
    queueManager = null;
  }
  
  @Test (expected = IllegalThreadStateException.class)
  public void threadFactoryReturnRunningThreadFail() {
    StartingThreadFactory threadFactory = new StartingThreadFactory();
    try {
      WorkerPool workerPool = new WorkerPool(threadFactory, 1);
      queueManager = new QueueManager(workerPool, THREAD_NAME, 
                                      AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    } finally {
      threadFactory.killThreads();
    }
  }
  
  @Test
  public void removeCallableTest() {
    TestCallable callable = new TestCallable();
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(new ListenableFutureTask<Object>(false, callable), 
                                                     null, Clock.lastKnownForwardProgressingMillis());
    
    assertFalse(queueManager.remove(callable));
    
    queueManager.highPriorityQueueSet.executeQueue.add(task);

    assertTrue(queueManager.remove(callable));
    assertFalse(queueManager.remove(callable));
    
    queueManager.lowPriorityQueueSet.scheduleQueue.addFirst(task);

    assertTrue(queueManager.remove(callable));
    assertFalse(queueManager.remove(callable));
  }
  
  @Test
  public void removeRunnableTest() {
    TestRunnable runnable = new TestRunnable();
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(runnable, null, 
                                                     Clock.lastKnownForwardProgressingMillis());
    
    assertFalse(queueManager.remove(runnable));
    
    queueManager.highPriorityQueueSet.executeQueue.add(task);

    assertTrue(queueManager.remove(runnable));
    assertFalse(queueManager.remove(runnable));
    
    queueManager.lowPriorityQueueSet.scheduleQueue.addFirst(task);

    assertTrue(queueManager.remove(runnable));
    assertFalse(queueManager.remove(runnable));
  }
  
  @Test
  public void getNextReadyTaskNotRunningTest() throws InterruptedException {
    queueManager.stop();
    
    assertNull(queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskExecuteOnlyHighTest() throws InterruptedException {
    getNextReadyTaskExecuteTest(queueManager.highPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskExecuteOnlyLowTest() throws InterruptedException {
    getNextReadyTaskExecuteTest(queueManager.lowPriorityQueueSet);
  }
  
  private void getNextReadyTaskExecuteTest(QueueSet queueSet) throws InterruptedException {
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), queueSet.executeQueue, 
                                                     Clock.lastKnownForwardProgressingMillis());
    
    queueSet.addExecute(task);
    
    assertTrue(task == queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskScheduleOnlyHighTest() throws InterruptedException {
    getNextReadyTaskScheduledTest(queueManager.highPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskScheduleOnlyLowTest() throws InterruptedException {
    getNextReadyTaskScheduledTest(queueManager.lowPriorityQueueSet);
  }
  
  private void getNextReadyTaskScheduledTest(QueueSet queueSet) throws InterruptedException {
    TaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), queueSet.scheduleQueue, 
                                              Clock.lastKnownForwardProgressingMillis());
    
    queueSet.addScheduled(task);
    
    assertTrue(task == queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskScheduleDelayTest() throws InterruptedException {
    long startTime = Clock.accurateForwardProgressingMillis();
    TaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), 
                                              queueManager.highPriorityQueueSet.scheduleQueue, 
                                              Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    queueManager.highPriorityQueueSet.addScheduled(task);
    
    TaskWrapper resultTask;
    resultTask = queueManager.getNextReadyTask();
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(task == resultTask);
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test
  public void getNextReadyTaskExecuteAheadOfScheduledTest() throws InterruptedException {
    OneTimeTaskWrapper executeTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                            queueManager.highPriorityQueueSet.executeQueue, 
                                                            Clock.accurateForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addExecute(executeTask);
    TestUtils.blockTillClockAdvances();
    TaskWrapper scheduleTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                      queueManager.highPriorityQueueSet.scheduleQueue, 
                                                      Clock.lastKnownForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);

    assertTrue(executeTask == queueManager.getNextReadyTask());
    assertTrue(scheduleTask == queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskScheduledAheadOfExecuteTest() throws InterruptedException {
    TaskWrapper scheduleTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                      queueManager.highPriorityQueueSet.scheduleQueue,
                                                      Clock.accurateForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);
    TestUtils.blockTillClockAdvances();
    OneTimeTaskWrapper executeTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                            queueManager.highPriorityQueueSet.executeQueue, 
                                                            Clock.lastKnownForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addExecute(executeTask);

    assertTrue(scheduleTask == queueManager.getNextReadyTask());
    assertTrue(executeTask == queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskHighPriorityDelayedTest() throws InterruptedException {
    TaskWrapper scheduleTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                      queueManager.highPriorityQueueSet.scheduleQueue, 
                                                      Clock.accurateForwardProgressingMillis() + 1000);
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);
    TestUtils.blockTillClockAdvances();
    OneTimeTaskWrapper executeTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                            queueManager.lowPriorityQueueSet.executeQueue, 
                                                            Clock.lastKnownForwardProgressingMillis());
    queueManager.lowPriorityQueueSet.addExecute(executeTask);

    assertTrue(executeTask == queueManager.getNextReadyTask());
  }
  
  @Test
  public void getNextReadyTaskHighPriorityReadyFirstTest() throws InterruptedException {
    long startTime = Clock.accurateForwardProgressingMillis();
    TaskWrapper highTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                  queueManager.highPriorityQueueSet.scheduleQueue, 
                                                  Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    TaskWrapper lowTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                 queueManager.lowPriorityQueueSet.scheduleQueue, 
                                                 Clock.lastKnownForwardProgressingMillis() + (DELAY_TIME * 10));
    queueManager.highPriorityQueueSet.addScheduled(highTask);
    queueManager.lowPriorityQueueSet.addScheduled(lowTask);

    assertTrue(highTask == queueManager.getNextReadyTask());
    long endTime = Clock.accurateForwardProgressingMillis();
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test
  public void getNextReadyTaskLowPriorityReadyFirstTest() throws InterruptedException {
    long startTime = Clock.accurateForwardProgressingMillis();
    TaskWrapper highTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                  queueManager.highPriorityQueueSet.scheduleQueue, 
                                                  Clock.accurateForwardProgressingMillis() + (DELAY_TIME * 10));
    TaskWrapper lowTask = new OneTimeTaskWrapper(new TestRunnable(), 
                                                 queueManager.lowPriorityQueueSet.scheduleQueue, 
                                                 Clock.lastKnownForwardProgressingMillis() + DELAY_TIME);
    queueManager.highPriorityQueueSet.addScheduled(highTask);
    queueManager.lowPriorityQueueSet.addScheduled(lowTask);

    assertTrue(lowTask == queueManager.getNextReadyTask());
    long endTime = Clock.accurateForwardProgressingMillis();
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    assertEquals(AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, queueManager.getMaxWaitForLowPriority());
    
    long lowPriorityWait = Long.MAX_VALUE;
    queueManager.setMaxWaitForLowPriority(lowPriorityWait);
    
    assertEquals(lowPriorityWait, queueManager.getMaxWaitForLowPriority());
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    try {
      queueManager.setMaxWaitForLowPriority(-1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    
    assertEquals(AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, queueManager.getMaxWaitForLowPriority());
  }
}
