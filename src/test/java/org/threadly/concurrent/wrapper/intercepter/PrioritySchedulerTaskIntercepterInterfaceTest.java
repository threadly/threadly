package org.threadly.concurrent.wrapper.intercepter;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTaskIntercepterInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PrioritySchedulerTaskIntercepterFactory();
  }

  private static class PrioritySchedulerTaskIntercepterFactory implements SchedulerServiceFactory {
    private List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(1);
    
    @Override
    public PrioritySchedulerTaskIntercepter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public PrioritySchedulerTaskIntercepter makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public PrioritySchedulerTaskIntercepter makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      schedulers.add(result);
      
      return new PrioritySchedulerTaskIntercepter(result) {
        @Override
        public Runnable wrapTask(Runnable task, boolean recurring) {
          return task;
        }
      };
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
