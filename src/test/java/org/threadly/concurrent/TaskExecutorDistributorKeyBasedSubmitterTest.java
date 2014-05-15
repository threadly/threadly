package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("javadoc")
public class TaskExecutorDistributorKeyBasedSubmitterTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyBasedSubmitterFactory();
  }

  private class KeyBasedSubmitterFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private KeyBasedSubmitterFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize, 
                                                            boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                               1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      return new TaskExecutorDistributor(executor).getSubmitterForKey("foo");
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
  
}
