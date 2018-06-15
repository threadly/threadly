package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class KeyedSchedulerServiceLimiterTest extends AbstractKeyedLimiterTest {
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
  protected KeyedSchedulerServiceLimiter makeLimiter(int limit) {
    return new KeyedSchedulerServiceLimiter(scheduler, limit, null, true, true);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new KeyedSchedulerServiceLimiter(null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyedSchedulerServiceLimiter(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableTest() {
    KeyedSchedulerServiceLimiter limiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertFalse(limiter.remove(btr));
      assertFalse(limiter.remove((Runnable)null));
      
      limiter.execute(key, btr);
      
      TestRunnable tr = new TestRunnable();
      
      assertFalse(limiter.remove(tr));

      limiter.execute(key, tr);
      assertTrue(limiter.remove(tr));
      assertFalse(limiter.remove(tr));

      limiter.submit(key, tr);
      assertTrue(limiter.remove(tr));
      assertFalse(limiter.remove(tr));
    } finally {
      btr.unblock();
    }
  }

  @Test
  public void removeCallableTest() {
    KeyedSchedulerServiceLimiter limiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertFalse(limiter.remove(btr));
      assertFalse(limiter.remove((Runnable)null));
      
      limiter.execute(key, btr);
      
      TestCallable tc = new TestCallable();
      
      assertFalse(limiter.remove(tc));

      limiter.submit(key, tc);
      assertTrue(limiter.remove(tc));
      assertFalse(limiter.remove(tc));
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void getActiveTaskCountTest() {
    KeyedSchedulerServiceLimiter limiter = makeLimiter(1);
    String key = StringUtils.makeRandomString(5);
    
    assertEquals(0, limiter.getActiveTaskCount());
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(key, btr);
      btr.blockTillStarted();
      
      assertEquals(1, limiter.getActiveTaskCount());
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void getQueuedTaskCountTest() {
    // must be single thread scheduler so we can block one on the shceduler
    KeyedSchedulerServiceLimiter limiter = new KeyedSchedulerServiceLimiter(new SingleThreadScheduler(), 1);
    String key = StringUtils.makeRandomString(5);
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, limiter.getQueuedTaskCount());
      
      limiter.execute(key, btr);
      btr.blockTillStarted();
      
      limiter.execute(StringUtils.makeRandomString(2), DoNothingRunnable.instance());
      // 1 blocked on scheduler due to different key
      assertEquals(1, limiter.getQueuedTaskCount());
      

      limiter.execute(key, DoNothingRunnable.instance());
      // 1 additional blocked in limiter now
      assertEquals(2, limiter.getQueuedTaskCount());
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void isShutdownTest() {
    KeyedSchedulerServiceLimiter limiter = makeLimiter(1);
    
    assertFalse(limiter.isShutdown());
    
    scheduler.shutdown();
    
    assertTrue(limiter.isShutdown());
  }
}
