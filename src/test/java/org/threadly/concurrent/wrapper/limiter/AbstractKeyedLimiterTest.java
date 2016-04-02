package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Map;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public abstract class AbstractKeyedLimiterTest {
  protected abstract AbstractKeyedLimiter<?> makeLimiter(int limit);
  
  @Test
  public void getMaxConcurrencyPerKeyTest() {
    assertEquals(1, makeLimiter(1).getMaxConcurrencyPerKey());
    int val = 10;
    assertEquals(val, makeLimiter(val).getMaxConcurrencyPerKey());
  }
  
  @Test
  public void getUnsubmittedTaskCountTest() {
    AbstractKeyedLimiter<?> singleConcurrencyLimiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, singleConcurrencyLimiter.getUnsubmittedTaskCount(key));
      singleConcurrencyLimiter.execute(key, btr);
      btr.blockTillStarted();
      // should not be queued any more
      assertEquals(0, singleConcurrencyLimiter.getUnsubmittedTaskCount(key));
      
      for (int i = 1; i < TEST_QTY; i++) {
        singleConcurrencyLimiter.submit(key, DoNothingRunnable.instance());
        assertEquals(i, singleConcurrencyLimiter.getUnsubmittedTaskCount(key));
      }
    } finally {
      btr.unblock();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUnsubmittedTaskCountNullFail() {
    makeLimiter(1).getUnsubmittedTaskCount(null);
  }
  
  @Test
  public void getUnsubmittedTaskCountMapTest() {
    AbstractKeyedLimiter<?> singleConcurrencyLimiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertTrue(singleConcurrencyLimiter.getUnsubmittedTaskCountMap().isEmpty());
      singleConcurrencyLimiter.execute(key, btr);
      btr.blockTillStarted();

      // should not be queued any more
      assertTrue(singleConcurrencyLimiter.getUnsubmittedTaskCountMap().isEmpty());
      
      for (int i = 1; i < TEST_QTY; i++) {
        singleConcurrencyLimiter.submit(key, DoNothingRunnable.instance());
        Map<?, ?> taskCountMap = singleConcurrencyLimiter.getUnsubmittedTaskCountMap();
        assertEquals(1, taskCountMap.size());
        assertEquals(i, taskCountMap.get(key));
      }
    } finally {
      btr.unblock();
    }
  }
}
