package org.threadly.concurrent.wrapper.compatibility;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class ScheduledExecutorServiceWrapperTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SchedulerFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }

  private class SchedulerFactory implements SubmitterSchedulerFactory {
    private final List<ScheduledThreadPoolExecutor> executors;
    
    private SchedulerFactory() {
      executors = new ArrayList<>(1);
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
