package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

@SuppressWarnings("javadoc")
public class ScheduledExecutorServiceWrapperTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SchedulerFactory();
  }

  private class SchedulerFactory implements SubmitterSchedulerFactory {
    private final List<ScheduledThreadPoolExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<ScheduledThreadPoolExecutor>();
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      return new ScheduledExecutorServiceWrapper(executor);
    }
    
    @Override
    public void shutdown() {
      Iterator<ScheduledThreadPoolExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }
  }
}
