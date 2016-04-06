package org.threadly.concurrent.limiter;

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

@SuppressWarnings({"javadoc", "deprecation"})
public class SchedulerServiceLimiterTest extends SimpleSchedulerLimiterTest {
  @Override
  protected SchedulerServiceLimiter getLimiter(int parallelCount) {
    return new SchedulerServiceLimiter(scheduler, parallelCount);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new SchedulerLimiterFactory(false);
  }
  
  @Override
  @Test
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
      assertTrue(limiter.getQueuedTaskCount() >= 1);
      
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
      assertTrue(limiter.getQueuedTaskCount() >= 1);
      
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
    private final boolean addSubPoolName;
    
    protected SchedulerLimiterFactory(boolean addSubPoolName) {
      this(Integer.MAX_VALUE, addSubPoolName);
    }
    
    private SchedulerLimiterFactory(int minLimiterAmount, boolean addSubPoolName) {
      schedulerFactory = new PrioritySchedulerFactory();
      this.minLimiterAmount = minLimiterAmount;
      this.addSubPoolName = addSubPoolName;
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
      
      if (addSubPoolName) {
        return new SchedulerServiceLimiter(scheduler, limiterAmount, "TestSubPool");
      } else {
        return new SchedulerServiceLimiter(scheduler, limiterAmount);
      }
    }
  }
}
