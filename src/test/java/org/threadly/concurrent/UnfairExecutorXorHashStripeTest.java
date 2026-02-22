package org.threadly.concurrent;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.UnfairExecutor.TaskHashXorTimeStripeGenerator;
import org.threadly.concurrent.UnfairExecutorTest.UnfairExecutorFactory;

@SuppressWarnings("javadoc")
public class UnfairExecutorXorHashStripeTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new UnfairExecutorFactory(TaskHashXorTimeStripeGenerator.instance());
  }
  
  @Test
  @Override
  public void executeInOrderTest() {
    // ignored, this test makes no sense for this executor
  }
  
}
