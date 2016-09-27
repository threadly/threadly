package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerTest extends AbstractPrioritySchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return new SingleThreadSchedulerFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SingleThreadScheduler(null);
  }
  
  @Test
  @Override
  public void scheduleWithFixedDelayTest() {
    recurringExecutionTest(false, true, true);
  }
  
  @Test
  @Override
  public void scheduleWithFixedDelayInitialDelayTest() {
    recurringExecutionTest(true, true, true);
  }
  
  @Test
  @Override
  public void scheduleAtFixedRateTest() {
    recurringExecutionTest(false, false, true);
  }
  
  @Test
  @Override
  public void scheduleAtFixedRateInitialDelayTest() {
    recurringExecutionTest(true, false, true);
  }
  
  @Test
  public void isShutdownTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    assertFalse(sts.isShutdown());
    
    executeTestRunnables(sts, 0);
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 0);
    sts.shutdownNow();
    
    assertTrue(sts.isShutdown());
  }
  
  @Test
  public void isTerminatedTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    assertFalse(sts.isTerminated());
    sts.shutdown();
    assertTrue(sts.isTerminated()); // termination should be immediate since no tasks were submitted
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    sts = new SingleThreadScheduler();
    try {
      sts.execute(btr);
      btr.blockTillStarted();
      sts.shutdown();
      
      // not terminated yet
      assertFalse(sts.isTerminated());
      
      btr.unblock();
      sts.awaitTermination();
      
      assertTrue(sts.isTerminated());
    } finally {
      btr.unblock();
      sts.shutdown();
    }
  }
  
  @Test
  public void shutdownTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(sts, 5).get(TEST_QTY - 1);
    
    sts.shutdown();
    
    // runnable should finish
    lastRunnable.blockTillFinished();
  }
  
  @Test
  public void shutdownRecurringTest() {
    final SingleThreadScheduler sts = new SingleThreadScheduler();
    TestRunnable tr = new TestRunnable();
    sts.scheduleWithFixedDelay(tr, 0, 0);
    
    tr.blockTillStarted();
    
    sts.shutdown();
    
    new TestCondition() {
      @Override
      public boolean get() {
        return ! sts.sManager.execThread.isAlive();
      }
    }.blockTillTrue();
  }
  
  @Test
  public void shutdownNowTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    
    // execute one runnable which will not complete
    BlockingTestRunnable btr = new BlockingTestRunnable();
    sts.execute(btr);

    try {
      List<TestRunnable> expectedRunnables = executeTestRunnables(sts, 0);
      
      btr.blockTillStarted();
      
      List<Runnable> canceledRunnables = sts.shutdownNow();
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
    }
  }
  
  @Test
  public void shutdownNowIgnoreCanceledFuturesTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      Runnable nonCanceledRunnable = new TestRunnable();
      sts.submitScheduled(nonCanceledRunnable, 1000 * 60 * 60);
      ListenableFuture<?> future = sts.submitScheduled(DoNothingRunnable.instance(), 
                                                       1000 * 60 * 60);
      
      future.cancel(false);
      
      List<Runnable> result = sts.shutdownNow();
      assertEquals(1, result.size()); // only canceled task removed
    } finally {
      sts.shutdown();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(sts, 5).get(TEST_QTY - 1);
    
    sts.shutdown();
    sts.awaitTermination();
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void awaitTerminationWithTimeoutTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(sts, 5).get(TEST_QTY - 1);

    sts.shutdown();
    assertTrue(sts.awaitTermination(1000 * 10));
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void awaitTerminationWithTimeoutExpireTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 1000 * 10);
    
    long start = Clock.accurateForwardProgressingMillis();
    sts.shutdown();
    assertFalse(sts.awaitTermination(DELAY_TIME));

    assertTrue(Clock.accurateForwardProgressingMillis() - start >= (DELAY_TIME - ALLOWED_VARIANCE));
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownExecutionFail() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    sts.shutdown();
    
    sts.execute(DoNothingRunnable.instance());
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownNowExecutionFail() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    sts.shutdownNow();
    
    sts.execute(DoNothingRunnable.instance());
  }

  private class SingleThreadSchedulerFactory implements AbstractPrioritySchedulerFactory {
    private final List<SingleThreadScheduler> schedulers = new LinkedList<SingleThreadScheduler>();

    @Override
    public void shutdown() {
      Iterator<SingleThreadScheduler> it = schedulers.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SingleThreadScheduler result = makeAbstractPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        if (prestartIfAvailable) {
          result.prestartExecutionThread(true);
        }
      }
      return result;
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize,
                                                                   TaskPriority defaultPriority,
                                                                   long maxWaitForLowPriority) {
      SingleThreadScheduler sts = new SingleThreadScheduler(defaultPriority, maxWaitForLowPriority);
      schedulers.add(sts);
      
      return sts;
    }

    @Override
    public SingleThreadScheduler makeAbstractPriorityScheduler(int poolSize) {
      SingleThreadScheduler sts = new SingleThreadScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}
