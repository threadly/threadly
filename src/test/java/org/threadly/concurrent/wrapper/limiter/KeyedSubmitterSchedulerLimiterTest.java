package org.threadly.concurrent.wrapper.limiter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.PriorityScheduler;

@SuppressWarnings("javadoc")
public class KeyedSubmitterSchedulerLimiterTest extends AbstractKeyedLimiterTest {
  protected PriorityScheduler scheduler;
  
  @BeforeEach
  public void setup() {
    scheduler = new PriorityScheduler(10);
  }
  
  @AfterEach
  public void cleanup() {
    scheduler.shutdownNow();
    scheduler = null;
  }

  @Override
  protected AbstractKeyedLimiter<?> makeLimiter(int limit) {
    return new KeyedSubmitterSchedulerLimiter(scheduler, limit, null, true, true);
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
