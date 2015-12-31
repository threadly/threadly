package org.threadly.concurrent;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class ThreadRenamingSchedulerServiceWrapperTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }

  private static class ThreadRenamingPoolWrapperFactory implements SchedulerServiceFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

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
      SchedulerService scheduler = schedulerFactory.makeSchedulerService(poolSize, prestartIfAvailable);

      return new ThreadRenamingSchedulerServiceWrapper(scheduler, "foo", false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
