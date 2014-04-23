package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.limiter.SimpleSchedulerLimiterTest.SchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class SimpleSchedulerLimiterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SchedulerLimiterFactory(false);
  }
}
