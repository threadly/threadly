package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTest extends AbstractPrioritySchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return getPrioritySchedulerFactory();
  }
  
  protected PrioritySchedulerFactory getPrioritySchedulerFactory() {
    return new PrioritySchedulerTestFactory();
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new PriorityScheduler(0, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PriorityScheduler(1, TaskPriority.High, -1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void constructorNullFactoryTest() {
    PriorityScheduler ps = new PriorityScheduler(1, TaskPriority.High, 1, null);
    // should be set with default
    assertNotNull(ps.workerPool.threadFactory);
  }
  
  @Test
  public void getAndSetPoolSizeTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    int corePoolSize = 1;
    PriorityScheduler scheduler = factory.makePriorityScheduler(corePoolSize);
    try {
      assertEquals(corePoolSize, scheduler.getMaxPoolSize());
      
      corePoolSize = 10;
      scheduler.setPoolSize(corePoolSize);
      
      assertEquals(corePoolSize, scheduler.getMaxPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setPoolSizeSmallerTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(10);
    try {
      scheduler.prestartAllThreads();
      
      scheduler.setPoolSize(1);
      
      assertEquals(1, scheduler.getMaxPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setPoolSizeFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    // first construct a valid scheduler
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    try {
      // verify no negative values
      try {
        scheduler.setPoolSize(-1);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException expected) {
        // ignored
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void increasePoolSizeWithWaitingTaskTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      scheduler.execute(btr);
      btr.blockTillStarted();
      // all these runnables should be blocked
      List<TestRunnable> executedRunnables = executeTestRunnables(scheduler, 0);
      
      scheduler.setPoolSize((TEST_QTY / 2) + 1); // this should allow the waiting test runnables to quickly execute
      
      Iterator<TestRunnable> it = executedRunnables.iterator();
      while (it.hasNext()) {
        it.next().blockTillStarted(); // will throw exception if not ran
      }
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }

  @Test
  public void getScheduledTaskCountTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getScheduledTaskCount());
      
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(4, result.getScheduledTaskCount());
      assertEquals(4, result.getScheduledTaskCount(null));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountLowPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(0, result.getScheduledTaskCount(TaskPriority.Low));
      
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.Low));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountHighPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1);
      // add directly to avoid starting the consumer
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.highPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
      
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      result.taskConsumer.lowPriorityQueueSet
            .executeQueue.add(new OneTimeTaskWrapper(DoNothingRunnable.instance(), null, 
                                                     Clock.lastKnownForwardProgressingMillis()));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getCurrentPoolSize());
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      
      tr.blockTillStarted();  // wait for execution
      
      assertEquals(1, scheduler.getCurrentPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void makeSubPoolTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(10);
    try {
      PrioritySchedulerService subPool = scheduler.makeSubPool(2);
      assertNotNull(subPool);
      assertTrue(subPool instanceof PrioritySchedulerLimiter);  // if true, test cases are covered under PrioritySchedulerLimiter unit cases
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeSubPoolFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    try {
      scheduler.makeSubPool(2);
      fail("Exception should have been thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void interruptedDuringRunTest() throws InterruptedException, TimeoutException {
    final long taskRunTime = 1000 * 10;
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      final AsyncVerifier interruptSentAV = new AsyncVerifier();
      TestRunnable tr = new TestRunnable() {
        @Override
        public void handleRunFinish() {
          long startTime = System.currentTimeMillis();
          Thread currentThread = Thread.currentThread();
          while (System.currentTimeMillis() - startTime < taskRunTime && 
                 ! currentThread.isInterrupted()) {
            // spin
          }
          
          interruptSentAV.assertTrue(currentThread.isInterrupted());
          interruptSentAV.signalComplete();
        }
      };
      
      ListenableFuture<?> future = scheduler.submit(tr);
      
      tr.blockTillStarted();
      new TestCondition() {
        @Override
        public boolean get() {
          synchronized (scheduler.workerPool.workersLock) {
            return scheduler.workerPool.waitingForWorker;
          }
        }
      }.blockTillTrue();
      assertEquals(1, scheduler.getCurrentPoolSize());
      
      // should interrupt
      assertTrue(future.cancel(true));
      interruptSentAV.waitForTest(); // verify thread was interrupted as expected
      
      // verify worker was returned to pool
      new TestCondition() {
        @Override
        public boolean get() {
          synchronized (scheduler.workerPool.workersLock) {
            return ! scheduler.workerPool.waitingForWorker;
          }
        }
      }.blockTillTrue();
      // verify pool size is still correct
      assertEquals(1, scheduler.getCurrentPoolSize());
      
      // verify interrupted status has been cleared
      final AsyncVerifier interruptClearedAV = new AsyncVerifier();
      scheduler.execute(new Runnable() {
        @Override
        public void run() {
          interruptClearedAV.assertFalse(Thread.currentThread().isInterrupted());
          interruptClearedAV.signalComplete();
        }
      });
      // block till we have verified that the interrupted status has been reset
      interruptClearedAV.waitForTest();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void isShutdownTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      assertFalse(scheduler.isShutdown());
      
      scheduler.shutdown();
      
      assertTrue(scheduler.isShutdown());
      
      scheduler = factory.makePriorityScheduler(1);
      scheduler.shutdownNow();
      
      assertTrue(scheduler.isShutdown());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      /* adding a run time to have greater chances that runnable 
       * will be waiting to execute after shutdown call.
       */
      TestRunnable lastRunnable = executeTestRunnables(scheduler, 5).get(TEST_QTY - 1);
      
      scheduler.shutdown();
      
      // runnable should finish
      lastRunnable.blockTillFinished();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownRecurringTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, 0);
      
      tr.blockTillStarted();
      
      scheduler.shutdown();
      
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.workerPool.isShutdownFinished() && scheduler.getCurrentPoolSize() == 0;
        }
      }.blockTillTrue();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      scheduler.shutdown();
      
      try {
        scheduler.execute(new TestRunnable());
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.schedule(new TestRunnable(), 1000, null);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.scheduleWithFixedDelay(new TestRunnable(), 100, 100);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownNowTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1);

      // execute one runnable which will not complete
      scheduler.execute(btr);
      btr.blockTillStarted();
      
      final List<TestRunnable> expectedRunnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        expectedRunnables.add(tr);
        scheduler.execute(tr, i % 2 == 0 ? TaskPriority.High : TaskPriority.Low);
      }
      
      List<Runnable> canceledRunnables = scheduler.shutdownNow();
      // unblock now so that others can run (if the unit test fails)
      btr.unblock();
      
      assertNotNull(canceledRunnables);
      assertTrue(canceledRunnables.containsAll(expectedRunnables));
      assertTrue(expectedRunnables.containsAll(canceledRunnables));
      
      Iterator<TestRunnable> it = expectedRunnables.iterator();
      while (it.hasNext()) {
        assertEquals(0, it.next().getRunCount());
      }
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownNowFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      scheduler.shutdownNow();
      
      try {
        scheduler.execute(new TestRunnable());
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.schedule(new TestRunnable(), 1000, null);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.scheduleWithFixedDelay(new TestRunnable(), 100, 100);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void addToQueueTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long taskDelay = 1000 * 10; // make it long to prevent it from getting consumed from the queue
    
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    try {
      scheduler.addToScheduleQueue(scheduler.taskConsumer.highPriorityQueueSet, 
                                   new OneTimeTaskWrapper(new TestRunnable(), null, 
                                                          Clock.lastKnownForwardProgressingMillis() + taskDelay));

      assertEquals(1, scheduler.taskConsumer.highPriorityQueueSet.scheduleQueue.size());
      assertEquals(0, scheduler.taskConsumer.lowPriorityQueueSet.scheduleQueue.size());
      
      assertTrue(scheduler.taskConsumer.isRunning());
      
      scheduler.addToScheduleQueue(scheduler.taskConsumer.lowPriorityQueueSet, 
                                   new OneTimeTaskWrapper(new TestRunnable(), null, 
                                                          Clock.lastKnownForwardProgressingMillis() + taskDelay));

      assertEquals(1, scheduler.taskConsumer.highPriorityQueueSet.scheduleQueue.size());
      assertEquals(1, scheduler.taskConsumer.lowPriorityQueueSet.scheduleQueue.size());
    } finally {
      factory.shutdown();
    }
  }
  
  public interface PrioritySchedulerFactory extends AbstractPrioritySchedulerFactory {
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                                                   long maxWaitForLowPriority);
    
    public PriorityScheduler makePriorityScheduler(int poolSize);
  }
  
  private static class PrioritySchedulerTestFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduler> executors;
    
    private PrioritySchedulerTestFactory() {
      executors = new LinkedList<PriorityScheduler>();
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = makePriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      
      return result;
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize,
                                                                   TaskPriority defaultPriority,
                                                                   long maxWaitForLowPriority) {
      return makePriorityScheduler(poolSize, defaultPriority, maxWaitForLowPriority);
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize) {
      return makePriorityScheduler(poolSize);
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority,
                                                   long maxWaitForLowPriority) {
      PriorityScheduler result = new StrictPriorityScheduler(poolSize, defaultPriority, 
                                                             maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int poolSize) {
      PriorityScheduler result = new StrictPriorityScheduler(poolSize);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
      }
    }
  }
}
