package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.AbstractPriorityScheduler.AccurateOneTimeTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSet;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueSetListener;
import org.threadly.concurrent.AbstractPriorityScheduler.TaskWrapper;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerQueueSetTest extends ThreadlyTester {
  private QueueSet queueSet;
  
  @BeforeEach
  public void setup() {
    queueSet = new QueueSet(new TestQueueSetListener());
  }
  
  @AfterEach
  public void cleanup() {
    queueSet = null;
  }
  
  @Test
  public void addExecuteTest() {
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                             Clock.lastKnownForwardProgressingMillis());
    
    queueSet.addExecute(task);
    
    assertEquals(1, queueSet.executeQueue.size());
    assertEquals(0, queueSet.scheduleQueue.size());
  }
  
  @Test
  public void addScheduledTest() {
    TaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                      Clock.lastKnownForwardProgressingMillis() + 10);
    
    queueSet.addScheduled(task);
    
    assertEquals(0, queueSet.executeQueue.size());
    assertEquals(1, queueSet.scheduleQueue.size());
  }
  
  @Test
  public void addScheduledOrderTest() {
    List<TaskWrapper> orderedList = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      orderedList.add(new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.accurateForwardProgressingMillis() + i));
    }
    List<TaskWrapper> randomList = new ArrayList<>(orderedList);
    Collections.shuffle(randomList);
    
    Iterator<TaskWrapper> it = randomList.iterator();
    while (it.hasNext()) {
      queueSet.addScheduled(it.next());
    }
    
    Iterator<TaskWrapper> expectedIt = orderedList.iterator();
    Iterator<TaskWrapper> resultIt = queueSet.scheduleQueue.iterator();
    while (expectedIt.hasNext()) {
      assertTrue(expectedIt.next() == resultIt.next());
    }
  }
  
  @Test
  public void removeCallableTest() {
    TestCallable callable = new TestCallable();
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(new ListenableFutureTask<>(callable), 
                                                             null, Clock.lastKnownForwardProgressingMillis());
    
    assertFalse(queueSet.remove(callable));
    
    queueSet.executeQueue.add(task);

    assertTrue(queueSet.remove(callable));
    assertFalse(queueSet.remove(callable));
    
    queueSet.scheduleQueue.addFirst(task);

    assertTrue(queueSet.remove(callable));
    assertFalse(queueSet.remove(callable));
  }
  
  @Test
  public void removeRunnableTest() {
    TestRunnable runnable = new TestRunnable();
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(runnable, null, 
                                                             Clock.lastKnownForwardProgressingMillis());
    
    assertFalse(queueSet.remove(runnable));
    
    queueSet.executeQueue.add(task);

    assertTrue(queueSet.remove(runnable));
    assertFalse(queueSet.remove(runnable));
    
    queueSet.scheduleQueue.addFirst(task);

    assertTrue(queueSet.remove(runnable));
    assertFalse(queueSet.remove(runnable));
  }
  
  @Test
  public void queueSizeTest() {
    assertEquals(0, queueSet.queueSize());
    
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                             Clock.lastKnownForwardProgressingMillis());
    
    queueSet.executeQueue.add(task);
    queueSet.scheduleQueue.addFirst(task);
    
    assertEquals(2, queueSet.queueSize());
  }
  
  @Test
  public void drainQueueIntoTest() {
    List<TaskWrapper> depositList = new ArrayList<>(2);
    
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                             Clock.lastKnownForwardProgressingMillis());
    
    queueSet.executeQueue.add(task);
    
    queueSet.drainQueueInto(depositList);
    
    assertTrue(depositList.contains(task));
    
    depositList.clear();
    
    queueSet.scheduleQueue.add(task);
    
    queueSet.drainQueueInto(depositList);
    
    assertTrue(depositList.contains(task));
  }
  
  @Test
  public void getNextTaskEmptyTest() {
    assertNull(queueSet.getNextTask());
  }
  
  @Test
  public void getNextTaskExecuteOnlyTest() {
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                             Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    queueSet.executeQueue.add(task);
    
    assertTrue(queueSet.getNextTask() == task);
  }
  
  @Test
  public void getNextTaskScheduleOnlyTest() {
    OneTimeTaskWrapper task = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                             Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    queueSet.scheduleQueue.add(task);
    
    assertTrue(queueSet.getNextTask() == task);
  }
  
  @Test
  public void getNextTaskExecuteFirstTest() {
    OneTimeTaskWrapper executeTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                                    Clock.accurateForwardProgressingMillis());
    OneTimeTaskWrapper scheduleTask = new AccurateOneTimeTaskWrapper(new TestRunnable(), null, 
                                                                     Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    queueSet.executeQueue.add(executeTask);
    queueSet.scheduleQueue.add(scheduleTask);
    
    assertTrue(queueSet.getNextTask() == executeTask);
  }
  
  @Test
  public void getNextTaskScheduleFirstTest() {
    OneTimeTaskWrapper executeTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                                    Clock.accurateForwardProgressingMillis() + DELAY_TIME);
    OneTimeTaskWrapper scheduleTask = new AccurateOneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                                     Clock.lastKnownForwardProgressingMillis());
    queueSet.executeQueue.add(executeTask);
    queueSet.scheduleQueue.add(scheduleTask);
    
    assertTrue(queueSet.getNextTask() == scheduleTask);
  }
  
  private static class TestQueueSetListener implements QueueSetListener {
    @Override
    public void handleQueueUpdate() {
      // ignored     
    }
  }
}
