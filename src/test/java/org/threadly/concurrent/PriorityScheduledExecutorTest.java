package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings({ "deprecation", "javadoc" })
public class PriorityScheduledExecutorTest extends PrioritySchedulerTest {
  @Override
  protected PrioritySchedulerFactory getPrioritySchedulerFactory() {
    return new PriorityScheduledExecutorTestFactory();
  }
  
  private static class PriorityScheduledExecutorTestFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private PriorityScheduledExecutorTestFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = makePriorityScheduler(poolSize, poolSize, Long.MAX_VALUE);
      if (prestartIfAvailable) {
        result.prestartAllCoreThreads();
      }
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize,
                                                   long keepAliveTimeInMs,
                                                   TaskPriority defaultPriority,
                                                   long maxWaitForLowPriority) {
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                       keepAliveTimeInMs, defaultPriority, 
                                                                       maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize, 
                                                   long keepAliveTimeInMs) {
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                       keepAliveTimeInMs);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
      }
    }
  }
}
