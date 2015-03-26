package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class KeyDistributedSchedulerKeySchedulerTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyBasedSubmitterSchedulerFactory();
  }
  
  @Test
  @Override
  public void scheduleWithFixedDelayTest() {
    recurringExecutionTest(false, true, true);
  }
  
  @Test
  @Override
  public void scheduleWithFixedDelayInitialDelayTest() {
    recurringExecutionTest(true, true, true);
  }
  
  @Test
  @Override
  public void scheduleAtFixedRateTest() {
    recurringExecutionTest(false, false, true);
  }
  
  @Test
  @Override
  public void scheduleAtFixedRateInitialDelayTest() {
    recurringExecutionTest(true, false, true);
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
      PriorityScheduler scheduler = new StrictPriorityScheduler(poolSize);
      executors.add(scheduler);
      if (prestartIfAvailable) {
        scheduler.prestartAllThreads();
      }
      
      KeyDistributedScheduler distributor = new KeyDistributedScheduler(poolSize, scheduler);
      
      return distributor.getSubmitterSchedulerForKey(this);
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
      }
      executors.clear();
    }
  }
}
