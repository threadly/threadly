package org.threadly.concurrent.wrapper.traceability;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterScheduler;

@SuppressWarnings("javadoc")
public class ThreadRenamingPrioritySchedulerTest extends ThreadRenamingSubmitterSchedulerTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  private static class ThreadRenamingPoolWrapperFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler scheduler = schedulerFactory.makePriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        scheduler.prestartAllThreads();
      }
      
      return new ThreadRenamingPriorityScheduler(scheduler, THREAD_NAME, false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
