package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("javadoc")
public class KeyDistributedExecutorKeySubmitterTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyBasedSubmitterFactory();
  }

  private class KeyBasedSubmitterFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduler> executors;
    
    private KeyBasedSubmitterFactory() {
      executors = new LinkedList<PriorityScheduler>();
    }
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllThreads();
      }
      executors.add(executor);
      
      return new KeyDistributedExecutor(executor).getSubmitterForKey("foo");
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
