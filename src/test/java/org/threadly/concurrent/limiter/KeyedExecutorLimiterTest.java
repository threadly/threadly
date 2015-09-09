package org.threadly.concurrent.limiter;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class KeyedExecutorLimiterTest extends AbstractKeyedLimiterTest {
  protected PriorityScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new PriorityScheduler(10);
  }
  
  @After
  public void cleanup() {
    scheduler.shutdownNow();
    scheduler = null;
  }

  @Override
  protected AbstractKeyedLimiter<?> makeLimiter(int limit) {
    return new KeyedExecutorLimiter(scheduler, limit, null, true, 1);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new KeyedExecutorLimiter(null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyedExecutorLimiter(SameThreadSubmitterExecutor.instance(), 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
