package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new SingleThreadSchedulerFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SingleThreadScheduler(null);
  }
  
  @Test
  public void isShutdownTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    assertFalse(sts.isShutdown());
    
    sts.shutdown();
    
    assertTrue(sts.isShutdown());
  }
  
  @Test (expected = IllegalStateException.class)
  public void shutdownExecutionFail() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    sts.shutdown();
    
    sts.execute(new TestRunnable());
  }

  private class SingleThreadSchedulerFactory implements SchedulerServiceFactory {
    private final List<SingleThreadScheduler> schedulers = new LinkedList<SingleThreadScheduler>();

    @Override
    public void shutdown() {
      Iterator<SingleThreadScheduler> it = schedulers.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
        it.remove();
      }
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SingleThreadScheduler sts = new SingleThreadScheduler();
      schedulers.add(sts);
      
      return sts;
    }
  }
}
