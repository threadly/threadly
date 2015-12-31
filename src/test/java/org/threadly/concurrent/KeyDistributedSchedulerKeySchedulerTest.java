package org.threadly.concurrent;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

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
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      KeyDistributedScheduler distributor = new KeyDistributedScheduler(poolSize, scheduler);
      
      return distributor.getSubmitterSchedulerForKey(this);
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
