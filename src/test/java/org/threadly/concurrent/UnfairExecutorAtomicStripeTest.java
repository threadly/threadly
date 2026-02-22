package org.threadly.concurrent;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.UnfairExecutor.AtomicStripeGenerator;
import org.threadly.concurrent.UnfairExecutorTest.UnfairExecutorFactory;

@SuppressWarnings("javadoc")
public class UnfairExecutorAtomicStripeTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new UnfairExecutorFactory(AtomicStripeGenerator.instance());
  }

  @Test
  @Override
  public void executeInOrderTest() {
    // ignored, this test makes no sense for this executor
  }
}
