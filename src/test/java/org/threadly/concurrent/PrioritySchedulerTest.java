package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.AccurateOneTimeTaskWrapper;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.priority.DefaultPriorityWrapper;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTest extends AbstractPrioritySchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return getPrioritySchedulerFactory();
  }
  
  protected PrioritySchedulerServiceFactory getPrioritySchedulerFactory() {
    return new PrioritySchedulerFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new PriorityScheduler(0, true);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PriorityScheduler(1, TaskPriority.High, -1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void constructorNullFactoryTest() {
    PriorityScheduler ps = new PriorityScheduler(1, TaskPriority.High, 1, true, null);
    // should be set with default
    assertNotNull(ps.workerPool.threadFactory);
  }
  
  @Test
  public void getAndSetPoolSizeTest() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      scheduler.execute(btr);
      btr.blockTillStarted();
      // all these runnables should be blocked
      List<TestRunnable> executedRunnables = executeTestRunnables(scheduler, 0);
      
      // this should allow the waiting test runnables to quickly execute
      scheduler.setPoolSize(Math.max(2, TEST_QTY / 2));
      
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
  @Ignore // TODO - test seems flakey still, unknown why
  public void taskPriorityThreadStartTest() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(2);  // use default of starvable does not start
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      // make sure the threads are exactly 2 and idle
      scheduler.prestartAllThreads();
      scheduler.setPoolSize(10);
      
      scheduler.execute(btr);
      // make sure only one thread wakes up before we submit a second task
      new TestCondition(() -> scheduler.workerPool.idleWorker.get() != null).blockTillTrue();
      scheduler.execute(btr);  // executed twice since effect is only after the second thread
      new TestCondition(() -> scheduler.workerPool.idleWorker.get() != null).blockTillTrue();
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Starvable);
      
      // should not execute even after delay
      TestUtils.sleep(DELAY_TIME);
      
      assertEquals(0, tr.getRunCount());
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
  public void interruptedDuringRunTest() throws InterruptedException, TimeoutException {
    final long taskRunTime = 1000 * 10;
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
      
      // should interrupt
      assertTrue(future.cancel(true));
      interruptSentAV.waitForTest(); // verify thread was interrupted as expected
      
      // verify worker was returned to pool
      new TestCondition(() -> scheduler.workerPool.idleWorker.get() != null).blockTillTrue();
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      /* adding a run time to have greater chances that runnable 
       * will be waiting to execute after shutdown call.
       */
      TestRunnable lastRunnable = 
          executeTestRunnables(new DefaultPriorityWrapper(scheduler, TaskPriority.Low), 5)
            .get(TEST_QTY - 1);
      
      scheduler.shutdown();
      
      // runnable should finish
      lastRunnable.blockTillFinished();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownRecurringTest() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, 0);
      
      tr.blockTillStarted();
      
      scheduler.shutdown();
      
      new TestCondition(() -> scheduler.workerPool.isShutdownFinished() && 
                                scheduler.getCurrentPoolSize() == 0).blockTillTrue();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownFail() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      scheduler.shutdown();
      
      try {
        scheduler.execute(DoNothingRunnable.instance());
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.schedule(DoNothingRunnable.instance(), 1000, null);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.scheduleWithFixedDelay(DoNothingRunnable.instance(), 100, 100);
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1);

      // execute one runnable which will not complete
      scheduler.execute(btr);
      btr.blockTillStarted();
      
      final List<TestRunnable> expectedRunnables = new ArrayList<>(TEST_QTY);
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
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      scheduler.shutdownNow();
      
      try {
        scheduler.execute(DoNothingRunnable.instance());
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.schedule(DoNothingRunnable.instance(), 1000, null);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
      try {
        scheduler.scheduleWithFixedDelay(DoNothingRunnable.instance(), 100, 100);
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownNowIgnoreCanceledFuturesTest() {

    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      Runnable nonCanceledRunnable = new TestRunnable();
      scheduler.submitScheduled(nonCanceledRunnable, 1000 * 60 * 60);
      ListenableFuture<?> future = scheduler.submitScheduled(DoNothingRunnable.instance(), 
                                                             1000 * 60 * 60);
      
      future.cancel(false);
      
      List<Runnable> result = scheduler.shutdownNow();
      assertEquals(1, result.size()); // only canceled task removed
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      TestRunnable tr = new TestRunnable(DELAY_TIME * 2);
      long start = Clock.accurateForwardProgressingMillis();
      scheduler.execute(tr);
      
      tr.blockTillStarted();
      scheduler.shutdown();
  
      scheduler.awaitTermination();
      long stop = Clock.accurateForwardProgressingMillis();
      
      assertTrue(stop - start >= (DELAY_TIME * 2) - 10);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTimeoutTest() throws InterruptedException {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      TestRunnable tr = new TestRunnable(DELAY_TIME * 2);
      long start = Clock.accurateForwardProgressingMillis();
      scheduler.execute(tr);
      
      tr.blockTillStarted();
      scheduler.shutdown();
  
      assertTrue(scheduler.awaitTermination(DELAY_TIME * 10));
      long stop = Clock.accurateForwardProgressingMillis();
      
      assertTrue(stop - start >= (DELAY_TIME * 2) - 10);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTimeoutExcededTest() throws InterruptedException {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1);
      
      TestRunnable tr = new TestRunnable(DELAY_TIME * 100);
      scheduler.execute(tr);
      tr.blockTillStarted();
      scheduler.shutdown();

      long start = Clock.accurateForwardProgressingMillis();
      assertFalse(scheduler.awaitTermination(DELAY_TIME));
      long stop = Clock.accurateForwardProgressingMillis();
      
      assertTrue(stop - start >= DELAY_TIME);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void addToQueueTest() {
    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    long taskDelay = 1000 * 10; // make it long to prevent it from getting consumed from the queue
    
    PriorityScheduler scheduler = factory.makePriorityScheduler(1);
    try {
      scheduler.queueScheduled(scheduler.queueManager.highPriorityQueueSet, 
                                   new AccurateOneTimeTaskWrapper(new TestRunnable(), null, 
                                                                  Clock.lastKnownForwardProgressingMillis() + taskDelay));

      assertEquals(1, scheduler.queueManager.highPriorityQueueSet.scheduleQueue.size());
      assertEquals(0, scheduler.queueManager.lowPriorityQueueSet.scheduleQueue.size());
      
      scheduler.queueScheduled(scheduler.queueManager.lowPriorityQueueSet, 
                                   new AccurateOneTimeTaskWrapper(new TestRunnable(), null, 
                                                                  Clock.lastKnownForwardProgressingMillis() + taskDelay));

      assertEquals(1, scheduler.queueManager.highPriorityQueueSet.scheduleQueue.size());
      assertEquals(1, scheduler.queueManager.lowPriorityQueueSet.scheduleQueue.size());
    } finally {
      factory.shutdown();
    }
  }

  @Test
  public void scheduleLaterThenSoonerTest() {
    // This test is focused around the scheduling defect fixed in 4.4.1
    // The condition hit was where we would park for one scheduled task, then a future task 
    // would not get executed in time because the first parked thread was not woken up

    PrioritySchedulerServiceFactory factory = getPrioritySchedulerFactory();
    final PriorityScheduler scheduler = factory.makePriorityScheduler(2);
    try {
      // schedule one task a ways out
      scheduler.schedule(DoNothingRunnable.instance(), 1000 * 60 * 10);
      // ensure first thread has blocked
      new TestCondition(() -> scheduler.workerPool.idleWorker.get() != null).blockTillTrue();
      
      // start second thread
      scheduler.prestartAllThreads();
      // ensure second thread has blocked
      new TestCondition(() -> scheduler.workerPool.idleWorker.get().nextIdleWorker != null)
        .blockTillTrue();
      
      // schedule soon to run task
      TestRunnable tr = new TestRunnable();
      scheduler.schedule(tr, 10);
      
      tr.blockTillStarted();
    } finally {
      factory.shutdown();
    }
  }
  
  public interface PrioritySchedulerServiceFactory extends AbstractPrioritySchedulerFactory {
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                                                   long maxWaitForLowPriority);
    
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                                                   long maxWaitForLowPriority, 
                                                   boolean stavableStartsThreads);
    
    public PriorityScheduler makePriorityScheduler(int poolSize);
  }
  
  public static class PrioritySchedulerFactory implements PrioritySchedulerServiceFactory {
    private final List<PriorityScheduler> executors;
    
    public PrioritySchedulerFactory() {
      executors = new ArrayList<>(2);
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
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority,
                                                   long maxWaitForLowPriority, 
                                                   boolean stavableStartsThreads) {
      PriorityScheduler result = new StrictPriorityScheduler(poolSize, defaultPriority, 
                                                             maxWaitForLowPriority, stavableStartsThreads);
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
        it.remove();
      }
    }
  }
}
