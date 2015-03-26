package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.QueueManager;
import org.threadly.concurrent.PriorityScheduler.RecurringDelayTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.RecurringTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.TaskWrapper;
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
    workerPool = new WorkerPool(threadFactory, 1, PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    queueManager = new QueueManager(workerPool, TaskPriority.High, THREAD_NAME) {
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
    WorkerPool workerPool = new WorkerPool(new StartingThreadFactory(), 1, 
                                           PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    queueManager = new QueueManager(workerPool, TaskPriority.High, THREAD_NAME);
    queueManager.start();
  }
  
  @Test
  public void removeCallableTest() {
    TestCallable callable = new TestCallable();
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(new ListenableFutureTask<Object>(false, callable), 0);
    
    assertFalse(queueManager.remove(callable));
    
    queueManager.executeQueue.add(task);

    assertTrue(queueManager.remove(callable));
    assertFalse(queueManager.remove(callable));
    
    queueManager.scheduleQueue.addFirst(task);

    assertTrue(queueManager.remove(callable));
    assertFalse(queueManager.remove(callable));
  }
  
  @Test
  public void removeRunnableTest() {
    TestRunnable runnable = new TestRunnable();
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(runnable, 0);
    
    assertFalse(queueManager.remove(runnable));
    
    queueManager.executeQueue.add(task);

    assertTrue(queueManager.remove(runnable));
    assertFalse(queueManager.remove(runnable));
    
    queueManager.scheduleQueue.addFirst(task);

    assertTrue(queueManager.remove(runnable));
    assertFalse(queueManager.remove(runnable));
  }
  
  @Test
  public void addExecuteTest() {
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), 0);
    
    queueManager.addExecute(task);
    
    assertTrue(queueManager.isRunning());
    assertEquals(1, queueManager.executeQueue.size());
    assertEquals(0, queueManager.scheduleQueue.size());
  }
  
  @Test
  public void addScheduledTest() {
    TaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), 10);
    
    queueManager.addScheduled(task);
    
    assertTrue(queueManager.isRunning());
    assertEquals(0, queueManager.executeQueue.size());
    assertEquals(1, queueManager.scheduleQueue.size());
  }
  
  @Test
  public void addScheduledOrderTest() {
    List<TaskWrapper> orderedList = new ArrayList<TaskWrapper>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      orderedList.add(new OneTimeTaskWrapper(new TestRunnable(), i));
    }
    List<TaskWrapper> randomList = new ArrayList<TaskWrapper>(orderedList);
    Collections.shuffle(randomList);
    
    Iterator<TaskWrapper> it = randomList.iterator();
    while (it.hasNext()) {
      queueManager.addScheduled(it.next());
    }
    
    Iterator<TaskWrapper> expectedIt = orderedList.iterator();
    Iterator<TaskWrapper> resultIt = queueManager.scheduleQueue.iterator();
    while (expectedIt.hasNext()) {
      assertTrue(expectedIt.next() == resultIt.next());
    }
  }
  
  @Test
  public void addScheduledLastTest() {
    RecurringTaskWrapper task = new RecurringDelayTaskWrapper(new TestRunnable(), queueManager, 10, 10);
    
    queueManager.addScheduledLast(task);
    
    assertFalse(queueManager.isRunning());
    assertEquals(0, queueManager.executeQueue.size());
    assertEquals(1, queueManager.scheduleQueue.size());
  }
  
  @Test
  public void getNextTaskNotRunningTest() throws InterruptedException {
    assertNull(queueManager.getNextTask());
  }
  
  @Test
  public void getNextTaskExecuteOnlyTest() throws InterruptedException {
    OneTimeTaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), 0);
    
    queueManager.addExecute(task);
    
    assertTrue(task == queueManager.getNextTask());
  }
  
  @Test
  public void getNextTaskScheduleOnlyTest() throws InterruptedException {
    TaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), 0);
    
    queueManager.addScheduled(task);
    
    assertTrue(task == queueManager.getNextTask());
  }
  
  @Test
  public void getNextTaskScheduleDelayTest() throws InterruptedException {
    TaskWrapper task = new OneTimeTaskWrapper(new TestRunnable(), DELAY_TIME);
    queueManager.addScheduled(task);
    
    TaskWrapper resultTask;
    long startTime = Clock.accurateForwardProgressingMillis();
    resultTask = queueManager.getNextTask();
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(task == resultTask);
    assertTrue((endTime - startTime) >= DELAY_TIME);
  }
  
  @Test
  public void getNextTaskExecuteAheadOfScheduledTest() throws InterruptedException {
    OneTimeTaskWrapper executeTask = new OneTimeTaskWrapper(new TestRunnable(), 0);
    queueManager.addExecute(executeTask);
    TestUtils.blockTillClockAdvances();
    TaskWrapper scheduleTask = new OneTimeTaskWrapper(new TestRunnable(), 0);
    queueManager.addScheduled(scheduleTask);

    assertTrue(executeTask == queueManager.getNextTask());
    assertTrue(scheduleTask == queueManager.getNextTask());
  }
  
  @Test
  public void getNextTaskScheduledAheadOfExecuteTest() throws InterruptedException {
    TaskWrapper scheduleTask = new OneTimeTaskWrapper(new TestRunnable(), 0);
    queueManager.addScheduled(scheduleTask);
    TestUtils.blockTillClockAdvances();
    OneTimeTaskWrapper executeTask = new OneTimeTaskWrapper(new TestRunnable(), 0);
    queueManager.addExecute(executeTask);

    assertTrue(scheduleTask == queueManager.getNextTask());
    assertTrue(executeTask == queueManager.getNextTask());
  }
}
