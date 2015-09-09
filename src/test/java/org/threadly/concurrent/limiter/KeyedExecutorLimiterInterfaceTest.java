package org.threadly.concurrent.limiter;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyedExecutorLimiterInterfaceTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyedExecutorLimiterFactory();
  }
  
  private static class KeyedExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize * 2);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);
      
      return new KeyedExecutorLimiter(ps, poolSize).getSubmitterExecutorForKey("foo");
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
