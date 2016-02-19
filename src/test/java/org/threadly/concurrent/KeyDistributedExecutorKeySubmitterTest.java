package org.threadly.concurrent;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;

@SuppressWarnings("javadoc")
public class KeyDistributedExecutorKeySubmitterTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyBasedSubmitterFactory();
  }

  private class KeyBasedSubmitterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable);
      
      return new KeyDistributedExecutor(executor).getExecutorForKey("foo");
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
