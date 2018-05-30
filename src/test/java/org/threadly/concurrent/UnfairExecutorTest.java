package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
import org.threadly.concurrent.UnfairExecutor.TaskStripeGenerator;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class UnfairExecutorTest extends ThreadlyTester {
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new UnfairExecutor(-1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException execpted) {
      // expected
    }
    try {
      new UnfairExecutor(13, (TaskStripeGenerator)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException execpted) {
      // expected
    }
  }
  
  @Test
  public void constructorTest() {
    assertTrue(new UnfairExecutor(13, true).schedulers[0].thread.isDaemon());
    assertFalse(new UnfairExecutor(13, false).schedulers[0].thread.isDaemon());
  }
  
  @Test
  public void isShutdownTest() {
    UnfairExecutor ue = new UnfairExecutor(1);
    assertFalse(ue.isShutdown());

    SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 0);
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
    TestRunnable lastRunnable = 
        SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 5).get(TEST_QTY - 1);
    
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
      List<TestRunnable> expectedRunnables = 
          SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 0);
      
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
    TestRunnable lastRunnable = 
        SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 5).get(TEST_QTY - 1);
    
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
    TestRunnable lastRunnable = 
        SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 5).get(TEST_QTY - 1);

    ue.shutdown();
    assertTrue(ue.awaitTermination(1000 * 10));
    
    // runnable should already be done
    assertTrue(lastRunnable.ranOnce());
  }
  
  @Test
  public void awaitTerminationWithTimeoutExpireTest() throws InterruptedException {
    UnfairExecutor ue = new UnfairExecutor(1);
    SubmitterExecutorInterfaceTest.executeTestRunnables(ue, 1000 * 10);
    
    long start = Clock.accurateForwardProgressingMillis();
    ue.shutdown();
    assertFalse(ue.awaitTermination(DELAY_TIME));

    assertTrue(Clock.accurateForwardProgressingMillis() - start >= (DELAY_TIME - ALLOWED_VARIANCE));
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownExecutionFail() {
    UnfairExecutor ue = new UnfairExecutor(1);
    ue.shutdown();
    
    ue.execute(DoNothingRunnable.instance());
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void shutdownNowExecutionFail() {
    UnfairExecutor ue = new UnfairExecutor(1);
    ue.shutdownNow();
    
    ue.execute(DoNothingRunnable.instance());
  }

  protected static class UnfairExecutorFactory implements SubmitterExecutorFactory {
    private final TaskStripeGenerator stripeGenerator;
    private List<UnfairExecutor> executors = new ArrayList<>(1);
    
    public UnfairExecutorFactory(TaskStripeGenerator stripeGenerator) {
      this.stripeGenerator = stripeGenerator;
    }
    
    @Override
    public UnfairExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      UnfairExecutor result = new UnfairExecutor(poolSize, stripeGenerator);
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
