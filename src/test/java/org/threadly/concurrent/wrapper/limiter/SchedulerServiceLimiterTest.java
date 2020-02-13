package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest.SchedulerServiceFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class SchedulerServiceLimiterTest extends SubmitterSchedulerLimiterTest {
  @Override
  protected SchedulerServiceLimiter getLimiter(int parallelCount, boolean limitFutureListenersExecution) {
    return new SchedulerServiceLimiter(scheduler, parallelCount, limitFutureListenersExecution);
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
  public void executeLimitQueueTest() {
    SchedulerServiceLimiter limitedExecutor = getLimiter(PARALLEL_COUNT, true);
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<>(PARALLEL_COUNT);
    try {
      for (int i = 0; i < PARALLEL_COUNT; i++) {
        BlockingTestRunnable btr = new BlockingTestRunnable();
        limitedExecutor.execute(btr);
        blockingRunnables.add(btr);
      }
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.blockTillStarted();
      }
      
      assertEquals(0, limitedExecutor.getQueuedTaskCount());
      limitedExecutor.execute(DoNothingRunnable.instance());
      TestUtils.sleep(DELAY_TIME);
      assertEquals(1, limitedExecutor.getQueuedTaskCount());
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
  }
  
  @Test
  public void removeRunnableFromQueueTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1, true);
      
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
      SchedulerServiceLimiter limiter = getLimiter(1, true);
      
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
  public void removeRunningRecurringFixedRateTask() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1, true);
      
      limiter.scheduleAtFixedRate(btr, 0, 100);
      btr.blockTillStarted();
      
      assertTrue(limiter.remove(btr));
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void removeRunningRecurringFixedDelayTask() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1, true);
      
      limiter.scheduleWithFixedDelay(btr, 0, 100);
      btr.blockTillStarted();
      
      assertTrue(limiter.remove(btr));
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void removeRunningRecurringMultipleInstancesTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      SchedulerServiceLimiter limiter = getLimiter(1, true);
      
      limiter.scheduleAtFixedRate(btr, 0, 100);
      btr.blockTillStarted();
      // a couple others that we should be able to remove individually
      limiter.scheduleAtFixedRate(btr, 0, 100);
      limiter.execute(btr);

      assertEquals(2, limiter.getQueuedTaskCount());
      assertTrue(limiter.remove(btr));
      assertEquals(2, limiter.getQueuedTaskCount());
      assertTrue(limiter.remove(btr));
      assertEquals(1, limiter.getQueuedTaskCount());
      assertTrue(limiter.remove(btr));
      assertEquals(0, limiter.getQueuedTaskCount());
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
