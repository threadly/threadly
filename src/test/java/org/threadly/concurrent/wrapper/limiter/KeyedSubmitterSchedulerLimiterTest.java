package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;

@SuppressWarnings("javadoc")
public class KeyedSubmitterSchedulerLimiterTest extends AbstractKeyedLimiterTest {
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
    return new KeyedSubmitterSchedulerLimiter(scheduler, limit, null, true, 1);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new KeyedSubmitterSchedulerLimiter(null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyedSubmitterSchedulerLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
