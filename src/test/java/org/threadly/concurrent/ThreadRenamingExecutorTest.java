package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("javadoc")
public class ThreadRenamingExecutorTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }

  private static class ThreadRenamingPoolWrapperFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);

      return new ThreadRenamingExecutor(ps, "foo", false);
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
