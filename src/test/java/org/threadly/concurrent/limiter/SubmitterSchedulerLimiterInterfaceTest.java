package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.limiter.SubmitterSchedulerLimiterTest.SchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerLimiterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SchedulerLimiterFactory(false);
  }
}
