package org.threadly.concurrent.wrapper.traceability;

import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ThreadRenamingExecutorTest extends SubmitterExecutorInterfaceTest {
  protected static final String THREAD_NAME = StringUtils.makeRandomString(5);
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ThreadRenamingPoolWrapperFactory();
  }
  
  @Test
  public void threadRenamedTest() throws InterruptedException, TimeoutException {
    SubmitterExecutorFactory schedulerFactory = getSubmitterExecutorFactory();
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

  private static class ThreadRenamingPoolWrapperFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable);

      return new ThreadRenamingExecutor(executor, THREAD_NAME, false);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
