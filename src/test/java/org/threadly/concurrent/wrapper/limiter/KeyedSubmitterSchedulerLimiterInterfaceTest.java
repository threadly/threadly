package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyedSubmitterSchedulerLimiterInterfaceTest  extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyedSubmitterSchedulerLimiterFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;
  }
  
  private static class KeyedSubmitterSchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      /* we must honor pool size of one due to how scheduled tasks are handled.  Since an extra 
       * task is used for scheduled tasks, execution order may switch if there is more than one 
       * thread.
       */
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize > 1 ? 
                                                                               poolSize * 2 : 1, 
                                                                             prestartIfAvailable);
      
      return new KeyedSubmitterSchedulerLimiter(scheduler, poolSize).getSubmitterSchedulerForKey("foo");
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}