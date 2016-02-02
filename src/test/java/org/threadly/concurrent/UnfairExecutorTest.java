package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class UnfairExecutorTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new UnfairExecutorFactory();
  }
  
  @Test
  @Override
  public void executeInOrderTest() {
    // ignored, this test makes no sense for this executor
  }
  
  @Test
  public void getMaxPoolSizeTest() {
    UnfairExecutor ue = new UnfairExecutor(TEST_QTY);
    assertEquals(TEST_QTY, ue.getMaxPoolSize());
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    UnfairExecutor ue = new UnfairExecutor(TEST_QTY);
    try {
      assertEquals(0, ue.getCurrentPoolSize());
      
      TestRunnable tr = new TestRunnable();
      ue.execute(tr);
      tr.blockTillStarted();
      assertEquals(1, ue.getCurrentPoolSize());
      
      ue.prestartAllThreads();
      assertEquals(TEST_QTY, ue.getCurrentPoolSize());
    } finally {
      ue.shutdownNow();
    }
  }
  
  @Test
  public void isShutdownTest() {
    UnfairExecutor ue = new UnfairExecutor(1);
    assertFalse(ue.isShutdown());

    executeTestRunnables(ue, 0);
    ue.shutdown();
    assertTrue(ue.isShutdown());
    
    ue = new UnfairExecutor(1);
    ue.shutdownNow();
    assertTrue(ue.isShutdown());
  }
  
  @Test
  public void shutdownTest() {
    UnfairExecutor ue = new UnfairExecutor(1);
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(ue, 5).get(TEST_QTY - 1);
    
    ue.shutdown();
    
    // runnable should finish
    lastRunnable.blockTillFinished();
  }
  
  @Test
  public void shutdownNowTest() {
    UnfairExecutor ue = new UnfairExecutor(1);
    
    // execute one runnable which will not complete
    BlockingTestRunnable btr = new BlockingTestRunnable();
    ue.execute(btr);

    try {
      List<TestRunnable> expectedRunnables = executeTestRunnables(ue, 0);
      
      btr.blockTillStarted();
      
      List<Runnable> canceledRunnables = ue.shutdownNow();
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
  public void awaitTerminationTest() throws InterruptedException {
    UnfairExecutor ue = new UnfairExecutor(1);
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(ue, 5).get(TEST_QTY - 1);
    
    ue.shutdown();
    ue.awaitTermination();
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void awaitTerminationWithTimeoutTest() throws InterruptedException {
    UnfairExecutor ue = new UnfairExecutor(1);
    /* adding a run time to have greater chances that runnable 
     * will be waiting to execute after shutdown call.
     */
    TestRunnable lastRunnable = executeTestRunnables(ue, 5).get(TEST_QTY - 1);

    ue.shutdown();
    assertTrue(ue.awaitTermination(1000 * 10));
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void awaitTerminationWithTimeoutExpireTest() throws InterruptedException {
    UnfairExecutor ue = new UnfairExecutor(1);
    executeTestRunnables(ue, 1000 * 10);
    
    long start = Clock.accurateForwardProgressingMillis();
    ue.shutdown();
    assertFalse(ue.awaitTermination(DELAY_TIME));

    assertTrue(Clock.accurateForwardProgressingMillis() - start >= (DELAY_TIME - ALLOWED_VARIANCE));
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownExecutionFail() {
    UnfairExecutor ue = new UnfairExecutor(1);
    ue.shutdown();
    
    ue.execute(new TestRunnable());
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownNowExecutionFail() {
    UnfairExecutor ue = new UnfairExecutor(1);
    ue.shutdownNow();
    
    ue.execute(new TestRunnable());
  }

  private static class UnfairExecutorFactory implements SubmitterExecutorFactory {
    private List<UnfairExecutor> executors = new ArrayList<UnfairExecutor>(1);
    
    @Override
    public UnfairExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      UnfairExecutor result = new UnfairExecutor(poolSize);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      for (UnfairExecutor ue : executors) {
        ue.shutdownNow();
      }
    }
  }
}
