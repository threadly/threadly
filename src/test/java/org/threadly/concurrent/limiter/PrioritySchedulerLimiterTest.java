package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest.SchedulerServiceFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.wrapper.PrioritySchedulerDefaultPriorityWrapper;

@SuppressWarnings({"javadoc", "deprecation"})
public class PrioritySchedulerLimiterTest extends SchedulerServiceLimiterTest {
  @Override
  protected PrioritySchedulerLimiter getLimiter(int parallelCount) {
    return new PrioritySchedulerLimiter(scheduler, parallelCount);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new PrioritySchedulerLimiterFactory(true, false);
  }
  
  @Override
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new PrioritySchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PrioritySchedulerLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getDefaultPriorityTest() {
    PriorityScheduler executor = new StrictPriorityScheduler(1, TaskPriority.Low, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
    
    executor = new StrictPriorityScheduler(1, TaskPriority.High, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
  }

  protected static class PrioritySchedulerLimiterFactory implements SchedulerServiceFactory {
    private final List<PriorityScheduler> executors;
    private final boolean addPriorityToCalls;
    private final boolean addSubPoolName;
    
    public PrioritySchedulerLimiterFactory(boolean addPriorityToCalls, 
                                           boolean addSubPoolName) {
      executors = new LinkedList<PriorityScheduler>();
      this.addPriorityToCalls = addPriorityToCalls;
      this.addSubPoolName = addSubPoolName;
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllThreads();
      }
      executors.add(executor);
      
      PrioritySchedulerLimiter limiter;
      if (addSubPoolName) {
        limiter = new PrioritySchedulerLimiter(executor, poolSize, "TestSubPool");
      } else {
        limiter = new PrioritySchedulerLimiter(executor, poolSize);
      }
      
      if (addPriorityToCalls) {
        // we wrap the limiter so all calls are providing a priority
        return new PrioritySchedulerDefaultPriorityWrapper(limiter, TaskPriority.High);
      } else {
        return limiter;
      }
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
