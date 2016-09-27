package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public abstract class AbstractPrioritySchedulerTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return getAbstractPrioritySchedulerFactory();
  }
  
  protected abstract AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory();
  
  @Test
  public void getDefaultPriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    TaskPriority priority = TaskPriority.High;
    try {
      AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1, priority, 1000);
      
      assertEquals(priority, scheduler.getDefaultPriority());
      
      priority = TaskPriority.Low;
      scheduler = factory.makeAbstractPriorityScheduler(1, priority, 1000);
      assertEquals(priority, scheduler.getDefaultPriority());
      
      priority = TaskPriority.Starvable;
      scheduler = factory.makeAbstractPriorityScheduler(1, priority, 1000);
      assertEquals(priority, scheduler.getDefaultPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void constructorNullPriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler executor = factory.makeAbstractPriorityScheduler(1, null, 1);
      
      assertTrue(executor.getDefaultPriority() == AbstractPriorityScheduler.DEFAULT_PRIORITY);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    long lowPriorityWait = 1000;
    AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.High, lowPriorityWait);
    try {
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
      
      lowPriorityWait = Long.MAX_VALUE;
      scheduler.setMaxWaitForLowPriority(lowPriorityWait);
      
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    long lowPriorityWait = 1000;
    AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.High, lowPriorityWait);
    try {
      try {
        scheduler.setMaxWaitForLowPriority(-1);
        fail("Exception should have thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
      
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
    } finally {
      factory.shutdown();
    }
  }

  @Test
  public void getQueuedTaskCountTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler result = factory.makeAbstractPriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getQueuedTaskCount());

      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(4, result.getQueuedTaskCount());
      assertEquals(4, result.getQueuedTaskCount(null));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getQueuedTaskCountStarvablePriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler result = factory.makeAbstractPriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(0, result.getQueuedTaskCount(TaskPriority.Starvable));

      result.getQueueManager().getQueueSet(TaskPriority.Starvable)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.Starvable)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getQueuedTaskCount(TaskPriority.Starvable));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getQueuedTaskCountLowPriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler result = factory.makeAbstractPriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(0, result.getQueuedTaskCount(TaskPriority.Low));

      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getQueuedTaskCount(TaskPriority.Low));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getQueuedTaskCountHighPriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler result = factory.makeAbstractPriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.High)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getQueuedTaskCount(TaskPriority.High));

      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.getQueueManager().getQueueSet(TaskPriority.Low)
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getQueuedTaskCount(TaskPriority.High));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  @Override
  public void executeTest() {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.executeTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      TestRunnable tr3 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr3, TaskPriority.Starvable);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr3, TaskPriority.Starvable);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr3.blockTillFinished(1000 * 1, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.submitRunnableTest();
      
      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);

      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      TestRunnable tr3 = new TestRunnable();
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr3, TaskPriority.Starvable);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr3, TaskPriority.Starvable);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr3.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Test
  @Override
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.submitRunnableWithResultTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      TestRunnable tr3 = new TestRunnable();
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      scheduler.submit(tr3, tr3, TaskPriority.Starvable);
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      scheduler.submit(tr3, tr3, TaskPriority.Starvable);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr3.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Test
  @Override
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.submitCallableTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      TestCallable tc3 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);
      scheduler.submit(tc3, TaskPriority.Starvable);
      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
      tc3.blockTillTrue(); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Test
  public void lowPriorityFlowControlTest() {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    final AtomicBoolean testRunning = new AtomicBoolean(true);
    try {
      final AbstractPriorityScheduler scheduler = priorityFactory.makeAbstractPriorityScheduler(1, TaskPriority.High, DELAY_TIME);
      
      new Runnable() {
        @Override
        public void run() {
          if (testRunning.get()) {
            while (scheduler.getQueueManager().getQueueSet(TaskPriority.High).executeQueue.size() < 5) {
              scheduler.execute(this, TaskPriority.High);
            }
          }
        }
      }.run();
      
      TestRunnable lowPriorityRunnable = new TestRunnable();
      scheduler.execute(lowPriorityRunnable, TaskPriority.Low);
      
      assertTrue(lowPriorityRunnable.getDelayTillFirstRun() >= DELAY_TIME);
    } finally {
      testRunning.set(false);
      priorityFactory.shutdown();
    }
  }
  
  @Test
  public void removeHighPriorityRecurringRunnableTest() {
    removeRecurringRunnableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityRecurringRunnableTest() {
    removeRecurringRunnableTest(TaskPriority.Low);
  }
  
  @Test
  public void removeStarvablePriorityRecurringRunnableTest() {
    removeRecurringRunnableTest(TaskPriority.Starvable);
  }
  
  private void removeRecurringRunnableTest(TaskPriority priority) {
    int runFrequency = 1;
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1);
      TestRunnable removedTask = new TestRunnable();
      TestRunnable keptTask = new TestRunnable();
      scheduler.scheduleWithFixedDelay(removedTask, 0, runFrequency, priority);
      scheduler.scheduleWithFixedDelay(keptTask, 0, runFrequency, priority);
      removedTask.blockTillStarted();
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(removedTask));
      
      // verify removed is no longer running, and the kept task continues to run
      int keptRunCount = keptTask.getRunCount();
      int runCount = removedTask.getRunCount();
      TestUtils.sleep(runFrequency * 10);

      // may be +1 if the task was running while the remove was called
      assertTrue(removedTask.getRunCount() == runCount || 
                 removedTask.getRunCount() == runCount + 1);
      
      assertTrue(keptTask.getRunCount() >= keptRunCount);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void removeHighPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.Low);
  }
  
  @Test
  public void removeStarvablePriorityRunnableTest() {
    removeRunnableTest(TaskPriority.Starvable);
  }
  
  private void removeRunnableTest(TaskPriority priority) {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1);
      TestRunnable removedTask = new TestRunnable();
      scheduler.submitScheduled(removedTask, 10 * 1000, priority);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(removedTask));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void removeHighPriorityCallableTest() {
    removeCallableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityCallableTest() {
    removeCallableTest(TaskPriority.Low);
  }
  
  @Test
  public void removeStarvablePriorityCallableTest() {
    removeCallableTest(TaskPriority.Starvable);
  }
  
  private void removeCallableTest(TaskPriority priority) {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1);
      TestCallable task = new TestCallable();
      scheduler.submitScheduled(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      factory.shutdown();
    }
  }
  
  public interface AbstractPrioritySchedulerFactory extends SchedulerServiceFactory {
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize, 
                                                                   TaskPriority defaultPriority, 
                                                                   long maxWaitForLowPriority);
    
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize);
  }
}
