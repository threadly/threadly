package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.wrapper.limiter.SchedulerServiceLimiterTest.SchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class SchedulerServiceLimiterInterfaceTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new SchedulerLimiterFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;  // not single threaded, but limit might cause execution delay
  }
}
