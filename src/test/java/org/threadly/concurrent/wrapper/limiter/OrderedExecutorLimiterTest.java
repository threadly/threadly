package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.wrapper.SubmitterExecutorAdapter;

@SuppressWarnings("javadoc")
public class OrderedExecutorLimiterTest extends ExecutorLimiterTest {
  @Override
  protected ExecutorLimiter getLimiter(int parallelCount, boolean limitFutureListenersExecution) {
    return new OrderedExecutorLimiter<>(scheduler, parallelCount, 
                                        (r1, r2) -> System.identityHashCode(r1) - System.identityHashCode(r2))
                 .limiter;
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new OrderedExecutorLimiterFactory();
  }
  
  @Test
  @SuppressWarnings("unused")
  @Override
  public void constructorFail() {
    try {
      new OrderedExecutorLimiter<>(null, 100, (i1, i2) -> 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new OrderedExecutorLimiter<>(SameThreadSubmitterExecutor.instance(), 0, (i1, i2) -> 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new OrderedExecutorLimiter<>(SameThreadSubmitterExecutor.instance(), 100, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @Override
  public void executeInOrderTest() throws InterruptedException, TimeoutException {
    // we should not be executing in order since order is defined as the identity hash
    // so instead we verify that the executeInOrderTest() will fail

    try {
      for (int i = 0; i < TEST_QTY; i++) {
        super.executeInOrderTest();
      }
      fail("executeInOrderTest wont fail");
    } catch (AsyncVerifier.TestFailure expectedFailure) {
      // break loop
    }
  }
  
  @Test
  @Override
  public void futureListenerUnlimitedTest() {
    // ignored, may not complete due to task order
  }
  
  @Test
  public void nonRunnableTypeExecuteTest() {
    nonRunnableTypeTest(false);
  }
  
  @Test
  public void nonRunnableTypeSubmitTest() {
    nonRunnableTypeTest(true);
  }
  
  private static void nonRunnableTypeTest(boolean submit) {
    TestableScheduler scheduler = new TestableScheduler();
    OrderedExecutorLimiter<TestRunnable> limiter = new OrderedExecutorLimiter<>(scheduler, 1, 
        (r1, r2) -> r1.getRunCount() - r2.getRunCount());
    List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      if (submit) {
        limiter.submit(tr);
      } else {
        limiter.execute(tr);
      }
    }
    
    assertEquals(TEST_QTY, scheduler.tick());
    for (TestRunnable tr : runnables) {
      assertEquals(1, tr.getRunCount());
    }
  }

  protected static class OrderedExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    
    protected OrderedExecutorLimiterFactory() {
      schedulerFactory = new PrioritySchedulerFactory();
    }
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize * 2, prestartIfAvailable);
      OrderedExecutorLimiter<Runnable> limiter = new OrderedExecutorLimiter<>(executor, poolSize, 
          (r1, r2) -> System.identityHashCode(r1) - System.identityHashCode(r2));
      
      return new SubmitterExecutorAdapter(limiter::execute);
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
