package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.wrapper.limiter.SubmitterSchedulerLimiterTest.SchedulerLimiterFactory;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerLimiterInterfaceTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SchedulerLimiterFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;  // not single threaded, but limit might cause execution delay
  }
}
