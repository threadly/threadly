package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyedSchedulerServiceLimiterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyedSchedulerServiceLimiterFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;
  }
  
  private static class KeyedSchedulerServiceLimiterFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      /* we must honor pool size of one due to how scheduled tasks are handled.  Since an extra 
       * task is used for scheduled tasks, execution order may switch if there is more than one 
       * thread.
       */
      SchedulerService scheduler = schedulerFactory.makeSchedulerService(poolSize > 1 ? 
                                                                           poolSize * 2 : 1, 
                                                                         prestartIfAvailable);
      
      return new KeyedSchedulerServiceLimiter(scheduler, poolSize).getSubmitterSchedulerForKey("foo");
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
