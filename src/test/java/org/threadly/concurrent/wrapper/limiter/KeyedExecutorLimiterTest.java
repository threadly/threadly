package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.UnfairExecutor;

@SuppressWarnings("javadoc")
public class KeyedExecutorLimiterTest extends AbstractKeyedLimiterTest {
  protected UnfairExecutor executor;
  
  @Before
  public void setup() {
    executor = new UnfairExecutor(13);
  }
  
  @After
  public void cleanup() {
    executor.shutdownNow();
    executor = null;
  }

  @Override
  protected AbstractKeyedLimiter<?> makeLimiter(int limit) {
    return new KeyedExecutorLimiter(executor, limit, null, true, 1);
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
