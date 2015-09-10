package org.threadly.concurrent.limiter;

import java.util.concurrent.ExecutionException;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ExecutorLimiterNamedTest extends ExecutorLimiterTest {
  @Override
  @SuppressWarnings("deprecation")
  protected ExecutorLimiter getLimiter(int parallelCount) {
    return new ExecutorLimiter(scheduler, parallelCount, "TestSubPool");
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorLimiterFactory(true);
  }
  
  @Override
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    submitRunnableTest(true);
  }
}
