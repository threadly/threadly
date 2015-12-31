package org.threadly.concurrent;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class ThreadRenamingExecutorWrapperTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }

  private static class ThreadRenamingPoolWrapperFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable);

      return new ThreadRenamingExecutorWrapper(executor, "foo", false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
