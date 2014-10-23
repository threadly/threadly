package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
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
    
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
    
    sts = new SingleThreadScheduler();
    sts.shutdownNow();
    
    assertTrue(sts.isShutdown());
  }
  
  @Test
  public void shutdownTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    TestRunnable lastRunnable = null;
    for (int i = 0; i < TEST_QTY; i++) {
      /* adding a run time to have chances that there will be 
       * runnables waiting to execute after shutdown call.
       */
      lastRunnable = new TestRunnable(5);
      sts.execute(lastRunnable);
    }
    
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
        return ! sts.sManager.get().execThread.isAlive();
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
      List<TestRunnable> expectedRunnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        expectedRunnables.add(tr);
        sts.execute(tr);
      }
      
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

  private class SingleThreadSchedulerFactory implements SchedulerServiceFactory {
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
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SingleThreadScheduler sts = new SingleThreadScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}
