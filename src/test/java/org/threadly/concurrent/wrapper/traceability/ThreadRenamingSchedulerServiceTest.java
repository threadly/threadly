package org.threadly.concurrent.wrapper.traceability;

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ThreadRenamingSchedulerServiceTest extends SchedulerServiceInterfaceTest {
  protected static final String THREAD_NAME = StringUtils.makeRandomString(5);
  
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }
  
  @Test
  public void threadRenamedTest() throws InterruptedException, TimeoutException {
    SubmitterSchedulerFactory schedulerFactory = getSubmitterSchedulerFactory();
    try {
      AsyncVerifier av = new AsyncVerifier();
      schedulerFactory.makeSubmitterExecutor(1, false).execute(() -> {
        av.assertTrue(Thread.currentThread().getName().startsWith(THREAD_NAME));
        av.signalComplete();
      });
      av.waitForTest();
    } finally {
      schedulerFactory.shutdown();
    }
  }

  private static class ThreadRenamingPoolWrapperFactory implements SchedulerServiceFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      SchedulerService scheduler = schedulerFactory.makeSchedulerService(poolSize, prestartIfAvailable);

      return new ThreadRenamingSchedulerService(scheduler, THREAD_NAME, false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
