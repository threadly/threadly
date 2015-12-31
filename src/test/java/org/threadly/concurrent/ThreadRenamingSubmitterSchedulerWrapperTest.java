package org.threadly.concurrent;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class ThreadRenamingSubmitterSchedulerWrapperTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  private static class ThreadRenamingPoolWrapperFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new ThreadRenamingSubmitterSchedulerWrapper(scheduler, "foo", false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
