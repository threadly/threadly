package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("javadoc")
public class ThreadRenamingSchedulerServiceWrapperTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }

  private static class ThreadRenamingPoolWrapperFactory implements SchedulerServiceFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);

      return new ThreadRenamingSchedulerServiceWrapper(ps, "foo", false);
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
