package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("javadoc")
public class TaskSchedulerDistributorKeyBasedSchedulerTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyBasedSubmitterSchedulerFactory();
  }

  private class KeyBasedSubmitterSchedulerFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduler> executors;
    
    private KeyBasedSubmitterSchedulerFactory() {
      executors = new LinkedList<PriorityScheduler>();
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      PriorityScheduler scheduler = new StrictPriorityScheduler(poolSize, poolSize, 
                                                                1000 * 10);
      executors.add(scheduler);
      if (prestartIfAvailable) {
        scheduler.prestartAllCoreThreads();
      }
      
      TaskSchedulerDistributor distributor = new TaskSchedulerDistributor(poolSize, scheduler);
      
      return distributor.getSubmitterSchedulerForKey(this);
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
  
}
