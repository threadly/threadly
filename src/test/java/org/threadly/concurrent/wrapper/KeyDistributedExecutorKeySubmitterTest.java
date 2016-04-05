package org.threadly.concurrent.wrapper;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;

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
