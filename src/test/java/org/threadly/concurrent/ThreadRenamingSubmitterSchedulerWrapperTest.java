package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("javadoc")
public class ThreadRenamingSubmitterSchedulerWrapperTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  private static class ThreadRenamingPoolWrapperFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);
      
      return new ThreadRenamingSubmitterSchedulerWrapper(ps, "foo", false);
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
