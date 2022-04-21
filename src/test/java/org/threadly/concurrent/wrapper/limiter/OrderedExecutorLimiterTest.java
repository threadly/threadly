package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.SubmitterExecutorAdapter;

@SuppressWarnings("javadoc")
public class OrderedExecutorLimiterTest extends ExecutorLimiterTest {
  protected OrderedExecutorLimiter<Runnable> getOrderedLimiter(int parallelCount, 
                                                               boolean limitFutureListenersExecution) {
    return new OrderedExecutorLimiter<>(scheduler, parallelCount, limitFutureListenersExecution, 
                                        (r1, r2) -> System.identityHashCode(r1) - System.identityHashCode(r2));
  }
  
  @Override
  protected ExecutorLimiter getLimiter(int parallelCount, boolean limitFutureListenersExecution) {
    return getOrderedLimiter(parallelCount, limitFutureListenersExecution).limiter;
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
  public void getAndSetMaxConcurrencyTest() {
    OrderedExecutorLimiter<Runnable> limiter = getOrderedLimiter(PARALLEL_COUNT, true);
    assertEquals(PARALLEL_COUNT, limiter.getMaxConcurrency());
    limiter.setMaxConcurrency(1);
    assertEquals(1, limiter.getMaxConcurrency());
  }
  
  @Test
  @Override
  public void increaseMaxConcurrencyTest() {
    OrderedExecutorLimiter<Runnable> limiter = getOrderedLimiter(1, true);

    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(btr);
      // block till started so that our entire limit is used up
      btr.blockTillStarted();
      
      TestRunnable tr = new TestRunnable();
      limiter.execute(tr);  // wont be able to run
      
      limiter.setMaxConcurrency(2);
      
      tr.blockTillFinished();  // should be able to complete now that limit was increased
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  @Override
  public void getUnsubmittedTaskCountTest() {
    OrderedExecutorLimiter<Runnable> limiter = getOrderedLimiter(1, true);
    
    assertEquals(0, limiter.getUnsubmittedTaskCount());
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(btr);
      // block till started, and first check should still be zero
      btr.blockTillStarted();
      
      for (int i = 0; i < TEST_QTY; i ++) {
        assertEquals(i, limiter.getUnsubmittedTaskCount());
        limiter.execute(DoNothingRunnable.instance());
      }
    } finally {
      btr.unblock();
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
  
  // will cause NPE if callable in future is cleared on cancel
  @Test
  public void sortAfterCancelingFutureTest() {
    TestableScheduler scheduler = new TestableScheduler();
    OrderedExecutorLimiter<TestRunnable> limiter = new OrderedExecutorLimiter<>(scheduler, 1, 
        (r1, r2) -> r1.getRunCount() - r2.getRunCount());

    limiter.execute(new TestRunnable());  // will be submitted to scheduler
    ListenableFuture<?> lf = limiter.submit(new TestRunnable());
    assertTrue(lf.cancel(false));
    
    limiter.execute(new TestRunnable());
    limiter.submit(new TestRunnable());
    
    // -1 task count because the canceled future was removed from the queue
    assertEquals(3, scheduler.tick());
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
