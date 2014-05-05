package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    SingleThreadScheduler executor = new SingleThreadScheduler();
    return new SingleThreadSchedulerServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SingleThreadSchedulerServiceWrapper(null);
    fail("Exception should have thrown");
  }

  @Override
  @Test (expected = UnsupportedOperationException.class)
  public void scheduleAtFixedRateTest() {
    super.scheduleAtFixedRateTest();
  }
  
  @Override
  @Test (expected = UnsupportedOperationException.class)
  public void scheduleAtFixedRateConcurrentTest() {
    super.scheduleAtFixedRateConcurrentTest();
  }
  
  @Override
  @Test (expected = UnsupportedOperationException.class)
  public void scheduleAtFixedRateExceptionTest() {
    super.scheduleAtFixedRateExceptionTest();
  }
  
  @Override
  @Test (expected = UnsupportedOperationException.class)
  public void scheduleAtFixedRateFail() {
    super.scheduleAtFixedRateFail();
  }
}
