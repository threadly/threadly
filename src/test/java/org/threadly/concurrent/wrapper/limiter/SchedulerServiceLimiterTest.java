package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest.SchedulerServiceFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SchedulerServiceLimiterTest extends SubmitterSchedulerLimiterTest {
  @Override
  protected SchedulerServiceLimiter getLimiter(int parallelCount) {
    return new SchedulerServiceLimiter(scheduler, parallelCount);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new SchedulerLimiterFactory();
  }
  
  @Test
  @Override
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new SchedulerServiceLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SchedulerServiceLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableFromQueueTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1);
      
      limiter.execute(btr);
      
      TestRunnable tr = new TestRunnable();
      assertFalse(limiter.remove(tr));
      
      limiter.submit(tr);
      // verify it is in queue
      assertTrue(limiter.waitingTasks.size() >= 1);
      
      assertTrue(limiter.remove(tr));
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void removeCallableFromQueueTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1);
      
      limiter.execute(btr);
      
      TestCallable tc = new TestCallable();
      assertFalse(limiter.remove(tc));
      
      limiter.submit(tc);
      // verify it is in queue
      assertTrue(limiter.waitingTasks.size() >= 1);
      
      assertTrue(limiter.remove(tc));
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void isShutdownTest() {
    PriorityScheduler executor = new StrictPriorityScheduler(1);
    try {
      SchedulerServiceLimiter limiter = new SchedulerServiceLimiter(executor, 1);
      
      assertFalse(limiter.isShutdown());
      executor.shutdownNow();
      assertTrue(limiter.isShutdown());
    } finally {
      executor.shutdownNow();
    }
  }
  
  protected static class SchedulerLimiterFactory implements SchedulerServiceFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    private final int minLimiterAmount;
    
    protected SchedulerLimiterFactory() {
      this(Integer.MAX_VALUE);
    }
    
    private SchedulerLimiterFactory(int minLimiterAmount) {
      schedulerFactory = new PrioritySchedulerFactory();
      this.minLimiterAmount = minLimiterAmount;
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
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
      SchedulerService scheduler = schedulerFactory.makeSchedulerService(poolSize, prestartIfAvailable);
      
      int limiterAmount = Math.min(minLimiterAmount, poolSize);
      
      return new SchedulerServiceLimiter(scheduler, limiterAmount);
    }
  }
}
