package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
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
  public void isShutdownTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    assertFalse(sts.isShutdown());
    
    executeTestRunnables(sts, 0);
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 0);
    sts.shutdownNow();
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 0);
    sts.shutdownAndAwaitTermination();
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 0);
    sts.shutdownAndAwaitTermination(100);
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 0);
    sts.shutdownNowAndAwaitTermination();
    
    assertTrue(sts.isShutdown());
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
  public void shutdownAndAwaitTerminationTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(sts, 5).get(TEST_QTY - 1);
    
    sts.shutdownAndAwaitTermination();
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void shutdownAndAwaitTerminationWithTimeoutTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(sts, 5).get(TEST_QTY - 1);
    
    assertTrue(sts.shutdownAndAwaitTermination(1000 * 10));
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void shutdownAndAwaitTerminationWithTimeoutExpireTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 1000 * 10);
    
    long start = Clock.accurateForwardProgressingMillis();
    assertFalse(sts.shutdownAndAwaitTermination(DELAY_TIME));
    
    assertTrue(Clock.accurateForwardProgressingMillis() - start >= DELAY_TIME);
  }
  
  @Test
  public void shutdownNowAndAwaitTerminationTest() throws InterruptedException {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    executeTestRunnables(sts, 5);
      
    List<Runnable> canceledRunnables = sts.shutdownNowAndAwaitTermination();
    
    assertNotNull(canceledRunnables);
    
    Iterator<Runnable> it = canceledRunnables.iterator();
    while (it.hasNext()) {
      assertEquals(0, ((TestRunnable)it.next()).getRunCount());
    }
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownExecutionFail() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    sts.shutdown();
    
    sts.execute(new TestRunnable());
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownNowExecutionFail() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    sts.shutdownNow();
    
    sts.execute(new TestRunnable());
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
      SchedulerService result = makeAbstractPriorityScheduler(1);
      if (prestartIfAvailable) {
        try {
          result.submit(DoNothingRunnable.instance()).get();
        } catch (Exception e) {
          throw new RuntimeException(e);
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
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize) {
      SingleThreadScheduler sts = new SingleThreadScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}
