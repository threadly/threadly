package org.threadly.concurrent.wrapper.traceability;

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.util.StringUtils;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.test.concurrent.AsyncVerifier;

@SuppressWarnings("javadoc")
public class ThreadRenamingSubmitterSchedulerTest extends SubmitterSchedulerInterfaceTest {
  protected static final String THREAD_NAME = StringUtils.makeRandomString(5);
  
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
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
  
  private static class ThreadRenamingPoolWrapperFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new ThreadRenamingSubmitterScheduler(scheduler, THREAD_NAME, false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
