package org.threadly.concurrent.wrapper.intercepter;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerTaskIntercepterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SubmitterSchedulerTaskIntercepterFactory();
  }

  private static class SubmitterSchedulerTaskIntercepterFactory implements SubmitterSchedulerFactory {
    private List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(1);
    
    @Override
    public SubmitterSchedulerTaskIntercepter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterSchedulerTaskIntercepter makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      schedulers.add(result);
      
      return new SubmitterSchedulerTaskIntercepter(result) {
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
