package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterScheduler;

@SuppressWarnings("javadoc")
public class KeyedRateLimiterExecutorInterfaceTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyedRateLimiterExecutorFactory();
  }
  
  private static class KeyedRateLimiterExecutorFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new KeyedRateLimiterExecutor(scheduler, 100_000, 600_000, null).getSubmitterExecutorForKey(new Object());
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
