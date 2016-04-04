package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;

@SuppressWarnings({"javadoc", "deprecation"})
public class SubmitterSchedulerLimiterTest extends ExecutorLimiterTest {
  @Override
  protected SubmitterSchedulerLimiter getLimiter(int parallelCount) {
    return new SubmitterSchedulerLimiter(scheduler, parallelCount);
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
      new SubmitterSchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SubmitterSchedulerLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  protected static class SchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    private final boolean addSubPoolName;
    
    public SchedulerLimiterFactory(boolean addSubPoolName) {
      schedulerFactory = new PrioritySchedulerFactory();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      if (addSubPoolName) {
        return new SubmitterSchedulerLimiter(scheduler, poolSize, "TestSubPool");
      } else {
        return new SubmitterSchedulerLimiter(scheduler, poolSize);
      }
    }
  }
}
