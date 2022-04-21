package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.AbstractPriorityScheduler.AccurateOneTimeTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.ImmediateTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueManager;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSet;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSetListener;
import org.threadly.concurrent.AbstractPriorityScheduler.TaskWrapper;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerQueueManagerTest extends ThreadlyTester {
  private QueueManager queueManager;
  
  @Before
  public void setup() {
    queueManager = new QueueManager(new QueueSetListener() {
      @Override
      public void handleQueueUpdate() {
        // ignore event
      }
    }, AbstractPriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  @After
  public void cleanup() {
    queueManager = null;
  }
  
  @Test
  public void removeCallableTest() {
    TestCallable callable = new TestCallable();
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(new ListenableFutureTask<>(callable), 
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
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(runnable, null, 
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
  public void getNextReadyTaskExecuteOnlyHighTest() {
    getNextReadyTaskExecuteTest(queueManager.highPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskExecuteOnlyLowTest() {
    getNextReadyTaskExecuteTest(queueManager.lowPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskExecuteOnlyStavableTest() {
    getNextReadyTaskExecuteTest(queueManager.starvablePriorityQueueSet);
  }
  
  private void getNextReadyTaskExecuteTest(QueueSet queueSet) {
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                             queueSet.executeQueue, 
                                                             Clock.lastKnownForwardProgressingMillis());
    
    queueSet.addExecute(task);
    
    assertTrue(task == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskIgnoreStarvablePriorityTest() {
    ImmediateTaskWrapper task = new ImmediateTaskWrapper(DoNothingRunnable.instance(), 
                                                         queueManager.starvablePriorityQueueSet.executeQueue);
    
    queueManager.starvablePriorityQueueSet.addExecute(task);
    
    assertEquals(null, queueManager.getNextTask(false));
  }
  
  @Test
  public void getNextReadyTaskScheduleOnlyHighTest() {
    getNextReadyTaskScheduledTest(queueManager.highPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskScheduleOnlyLowTest() {
    getNextReadyTaskScheduledTest(queueManager.lowPriorityQueueSet);
  }
  
  @Test
  public void getNextReadyTaskScheduleOnlyStaravableTest() {
    getNextReadyTaskScheduledTest(queueManager.starvablePriorityQueueSet);
  }
  
  private void getNextReadyTaskScheduledTest(QueueSet queueSet) {
    TaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                      queueSet.scheduleQueue, 
                                                      Clock.lastKnownForwardProgressingMillis());
    
    queueSet.addScheduled(task);
    
    assertTrue(task == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskExecuteAheadOfScheduledTest() {
    OneTimeTaskWrapper executeTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                                    queueManager.highPriorityQueueSet.executeQueue, 
                                                                    Clock.accurateForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addExecute(executeTask);
    TestUtils.blockTillClockAdvances();
    TaskWrapper scheduleTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                              queueManager.highPriorityQueueSet.scheduleQueue, 
                                                              Clock.lastKnownForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);

    assertTrue(executeTask == queueManager.getNextTask(true));
    assertTrue(executeTask == queueManager.getNextTask(true));  // execute task has not been removed yet
    // this should remove the execute task so we can get the scheduled task
    assertTrue(executeTask.canExecute(executeTask.getExecuteReference()));
    assertTrue(scheduleTask == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskScheduledAheadOfExecuteTest() {
    TaskWrapper scheduleTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                              queueManager.highPriorityQueueSet.scheduleQueue,
                                                              Clock.accurateForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);
    TestUtils.blockTillClockAdvances();
    OneTimeTaskWrapper executeTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                                    queueManager.highPriorityQueueSet.executeQueue, 
                                                                    Clock.lastKnownForwardProgressingMillis());
    queueManager.highPriorityQueueSet.addExecute(executeTask);

    assertTrue(scheduleTask == queueManager.getNextTask(true));
    assertTrue(scheduleTask == queueManager.getNextTask(true));  // schedule task has not been removed yet
 // this should remove the schedule task so we can get the execute task
    assertTrue(scheduleTask.canExecute(executeTask.getExecuteReference()));
    assertTrue(executeTask == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskHighPriorityDelayedTest() {
    TaskWrapper scheduleTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                              queueManager.highPriorityQueueSet.scheduleQueue, 
                                                              Clock.accurateForwardProgressingMillis() + 1000);
    queueManager.highPriorityQueueSet.addScheduled(scheduleTask);
    TestUtils.blockTillClockAdvances();
    OneTimeTaskWrapper executeTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                                    queueManager.lowPriorityQueueSet.executeQueue, 
                                                                    Clock.lastKnownForwardProgressingMillis());
    queueManager.lowPriorityQueueSet.addExecute(executeTask);

    assertTrue(executeTask == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskHighPriorityReadyFirstTest() {
    TaskWrapper highTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                          queueManager.highPriorityQueueSet.scheduleQueue, 
                                                          Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    TaskWrapper lowTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                         queueManager.lowPriorityQueueSet.scheduleQueue, 
                                                         Clock.lastKnownForwardProgressingMillis() + (DELAY_TIME * 10));
    queueManager.highPriorityQueueSet.addScheduled(highTask);
    queueManager.lowPriorityQueueSet.addScheduled(lowTask);

    assertTrue(highTask == queueManager.getNextTask(true));
  }
  
  @Test
  public void getNextReadyTaskLowPriorityReadyFirstTest() {
    TaskWrapper highTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                          queueManager.highPriorityQueueSet.scheduleQueue, 
                                                          Clock.accurateForwardProgressingMillis() + (DELAY_TIME * 10));
    TaskWrapper lowTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                         queueManager.lowPriorityQueueSet.scheduleQueue, 
                                                         Clock.lastKnownForwardProgressingMillis() + DELAY_TIME);
    queueManager.highPriorityQueueSet.addScheduled(highTask);
    queueManager.lowPriorityQueueSet.addScheduled(lowTask);

    assertTrue(lowTask == queueManager.getNextTask(true));
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
