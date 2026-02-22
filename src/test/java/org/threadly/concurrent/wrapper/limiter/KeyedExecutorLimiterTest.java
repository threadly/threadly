package org.threadly.concurrent.wrapper.limiter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.UnfairExecutor;

@SuppressWarnings("javadoc")
public class KeyedExecutorLimiterTest extends AbstractKeyedLimiterTest {
  protected UnfairExecutor executor;
  
  @BeforeEach
  public void setup() {
    executor = new UnfairExecutor(13);
  }
  
  @AfterEach
  public void cleanup() {
    executor.shutdownNow();
    executor = null;
  }

  @Override
  protected AbstractKeyedLimiter<?> makeLimiter(int limit) {
    return new KeyedExecutorLimiter(executor, limit, null, true, true);
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
