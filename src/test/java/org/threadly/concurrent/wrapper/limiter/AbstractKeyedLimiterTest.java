package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Map;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public abstract class AbstractKeyedLimiterTest {
  protected abstract AbstractKeyedLimiter<?> makeLimiter(int limit);
  
  @Test
  public void getAndSetMaxConcurrencyPerKeyTest() {
    int val = 10;
    AbstractKeyedLimiter<?> limiter = makeLimiter(val);
    assertEquals(val, limiter.getMaxConcurrencyPerKey());
    limiter.setMaxConcurrencyPerKey(1);
    assertEquals(1, limiter.getMaxConcurrencyPerKey());
  }
  
  @Test
  public void increaseMaxConcurrencyTest() {
    AbstractKeyedLimiter<?> limiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(key, btr);
      // block till started so that our entire limit is used up
      btr.blockTillStarted();
      
      TestRunnable tr = new TestRunnable();
      limiter.execute(key, tr);  // wont be able to run
      
      limiter.setMaxConcurrencyPerKey(2);
      
      tr.blockTillFinished();  // should be able to complete now that limit was increased
    } finally {
      btr.unblock();
    }
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
  
  @Test
  public void getTrackedKeyCountTest() {
    AbstractKeyedLimiter<?> limiter = makeLimiter(1);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, limiter.getTrackedKeyCount());
      limiter.execute(btr, btr);
      assertEquals(1, limiter.getTrackedKeyCount());
      btr.unblock();
      btr.blockTillFinished();
      assertEquals(0, limiter.getTrackedKeyCount());
    } finally {
      btr.unblock();
    }
    
  }
}
