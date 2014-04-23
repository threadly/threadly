package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.limiter.SchedulerServiceLimiterTest.SchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class SchedulerServiceLimiterInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new SchedulerLimiterFactory(false);
  }
}
