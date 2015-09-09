package org.threadly.concurrent.limiter;

import java.util.ArrayList;
import java.util.List;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;

@SuppressWarnings("javadoc")
public class KeyedSchedulerServiceLimiterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new KeyedSchedulerServiceLimiterFactory();
  }
  
  private static class KeyedSchedulerServiceLimiterFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);
    
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
      PriorityScheduler ps = new PriorityScheduler(poolSize > 1 ? poolSize * 2 : 1);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);
      
      return new KeyedSchedulerServiceLimiter(ps, poolSize).getSubmitterSchedulerForKey("foo");
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
