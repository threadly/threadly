package org.threadly.concurrent.wrapper.interceptor;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTaskInterceptorInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PrioritySchedulerTaskInterceptorFactory();
  }

  private static class PrioritySchedulerTaskInterceptorFactory implements SchedulerServiceFactory {
    private List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(1);
    
    @Override
    public PrioritySchedulerTaskInterceptor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public PrioritySchedulerTaskInterceptor makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public PrioritySchedulerTaskInterceptor makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      schedulers.add(result);
      
      return new PrioritySchedulerTaskInterceptor(result) {
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
