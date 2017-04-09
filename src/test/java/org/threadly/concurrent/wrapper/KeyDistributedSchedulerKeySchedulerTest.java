package org.threadly.concurrent.wrapper;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyDistributedSchedulerKeySchedulerTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyBasedSubmitterSchedulerFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;
  }

  private class KeyBasedSubmitterSchedulerFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterScheduler makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      KeyDistributedScheduler distributor = new KeyDistributedScheduler(poolSize, scheduler);
      
      return distributor.getSchedulerForKey(this);
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
