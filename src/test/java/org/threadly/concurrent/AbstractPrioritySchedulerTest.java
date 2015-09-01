package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

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
  public void makeWithDefaultPriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    TaskPriority originalPriority = TaskPriority.Low;
    TaskPriority newPriority = TaskPriority.High;
    AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1, originalPriority, 1000);
    assertTrue(scheduler.makeWithDefaultPriority(originalPriority) == scheduler);
    PrioritySchedulerService newScheduler = scheduler.makeWithDefaultPriority(newPriority);
    try {
      assertEquals(newPriority, newScheduler.getDefaultPriority());
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
  public void getCurrentRunningCountTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    final AbstractPriorityScheduler scheduler = factory.makeAbstractPriorityScheduler(1);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getCurrentRunningCount());
      
      BlockingTestRunnable btr = new BlockingTestRunnable();
      scheduler.execute(btr);
      
      btr.blockTillStarted();
      
      assertEquals(1, scheduler.getCurrentRunningCount());
      
      btr.unblock();
      
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentRunningCount() == 0;
        }
      }.blockTillTrue();
    } finally {
      factory.shutdown();
    }
  }
  
  @Override
  @Test
  public void executeTest() {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.executeTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
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
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.submitRunnableWithResultTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    AbstractPrioritySchedulerFactory priorityFactory = getAbstractPrioritySchedulerFactory();
    try {
      super.submitCallableTest();

      PrioritySchedulerService scheduler = priorityFactory.makeAbstractPriorityScheduler(2);
      
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);
      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
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
            while (scheduler.getQueueSet(TaskPriority.High).executeQueue.size() < 5) {
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
  
  @Test
  public void wrapperSamePriorityTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler highPriorityScheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.High, 200);
      assertTrue(highPriorityScheduler.makeWithDefaultPriority(TaskPriority.High) == highPriorityScheduler);
      
      AbstractPriorityScheduler lowPriorityScheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.Low, 200);
      assertTrue(lowPriorityScheduler.makeWithDefaultPriority(TaskPriority.Low) == lowPriorityScheduler);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void wrapperTest() {
    AbstractPrioritySchedulerFactory factory = getAbstractPrioritySchedulerFactory();
    try {
      AbstractPriorityScheduler highPriorityScheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.High, 200);
      assertTrue(highPriorityScheduler.makeWithDefaultPriority(TaskPriority.Low).getDefaultPriority() == TaskPriority.Low);
      
      AbstractPriorityScheduler lowPriorityScheduler = factory.makeAbstractPriorityScheduler(1, TaskPriority.Low, 200);
      assertTrue(lowPriorityScheduler.makeWithDefaultPriority(TaskPriority.High).getDefaultPriority() == TaskPriority.High);
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
