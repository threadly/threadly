package org.threadly.concurrent.wrapper.interceptor;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;

@SuppressWarnings("javadoc")
public class SchedulerServiceTaskInterceptorInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new SchedulerServiceTaskInterceptorFactory();
  }

  private static class SchedulerServiceTaskInterceptorFactory implements SchedulerServiceFactory {
    private List<PriorityScheduler> schedulers = new ArrayList<>(1);
    
    @Override
    public SchedulerServiceTaskInterceptor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceTaskInterceptor makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceTaskInterceptor makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      schedulers.add(result);
      
      return new SchedulerServiceTaskInterceptor(result) {
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
