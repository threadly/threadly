package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiterTest.PrioritySchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class PrioritySchedulerLimiterInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PrioritySchedulerLimiterFactory(true, false);
  }
}
