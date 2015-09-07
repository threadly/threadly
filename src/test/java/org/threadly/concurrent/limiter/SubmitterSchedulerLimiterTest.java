package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerLimiterTest extends ExecutorLimiterTest {
  @Override
  protected SubmitterSchedulerLimiter getLimiter(int parallelCount) {
    return new SubmitterSchedulerLimiter(scheduler, parallelCount);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new SchedulerLimiterFactory(false);
  }
  
  @Override
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new SubmitterSchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new SubmitterSchedulerLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  protected static class SchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduler> executors;
    private final boolean addSubPoolName;
    
    public SchedulerLimiterFactory(boolean addSubPoolName) {
      executors = new LinkedList<PriorityScheduler>();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllThreads();
      }
      executors.add(executor);
      
      if (addSubPoolName) {
        return new SubmitterSchedulerLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new SubmitterSchedulerLimiter(executor, poolSize);
      }
    }
  }
}
