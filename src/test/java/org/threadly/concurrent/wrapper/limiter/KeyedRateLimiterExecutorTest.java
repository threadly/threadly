package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.threadly.TestConstants.SLOW_MACHINE;
import static org.threadly.TestConstants.TEST_PROFILE;
import static org.threadly.TestConstants.TEST_QTY;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.threadly.TestConstants.TestLoad;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class KeyedRateLimiterExecutorTest extends SubmitterExecutorInterfaceTest {
  private TestableScheduler scheduler;
  private KeyedRateLimiterExecutor limiter;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    limiter = new KeyedRateLimiterExecutor(scheduler, 1);
  }

  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new KeyedRateLimiterFactory();
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new KeyedRateLimiterExecutor(null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyedRateLimiterExecutor(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getCurrentMinimumDelayTest() {
    Object key = new Object();
    assertEquals(0, limiter.getMinimumDelay(key));
    
    limiter.execute(10, key, DoNothingRunnable.instance());
    int delay = limiter.getMinimumDelay(key);
    assertEquals(10000, delay, 1000);
    
    limiter.execute(10, key, DoNothingRunnable.instance());
    delay = limiter.getMinimumDelay(key);
    assertEquals(20000, delay, 1000);
  }
  
  @Test
  public void getFutureTillDelayTest() {
    Object key = new Object();
    // verify that an empty limiter returns a finished future
    ListenableFuture<?> f = limiter.getFutureTillDelay(key, 0);
    assertTrue(f.isDone());
    
    // verify it works if the limiter has waiting tasks
    limiter.execute(key, DoNothingRunnable.instance());
    f = limiter.getFutureTillDelay(key, 0);
    assertFalse(f.isDone());
    
    scheduler.advance(1000);
    assertTrue(f.isDone());
  }
  
  @Test
  public void limitTest() throws InterruptedException, ExecutionException {
    Object key = new Object();
    int rateLimit = 100;
    final AtomicInteger ranPermits = new AtomicInteger();
    PriorityScheduler pse = new StrictPriorityScheduler(32);
    try {
      KeyedRateLimiterExecutor krls = new KeyedRateLimiterExecutor(pse, rateLimit);
      ListenableFuture<?> lastFuture = null;
      double startTime = Clock.accurateForwardProgressingMillis();
      boolean flip = true;
      for (int i = 0; i < TEST_QTY * 2; i++) {
        final int permit = 5;
        if (flip) {
          lastFuture = krls.submit(permit, key, new Runnable() {
            @Override
            public void run() {
              ranPermits.addAndGet(permit);
            }
          });
          flip = false;
        } else {
          lastFuture = krls.submit(permit, key, new Callable<Void>() {
            @Override
            public Void call() {
              ranPermits.addAndGet(permit);
              return null;
            }
          });
          flip = true;
        }
      }
      lastFuture.get();
      long endTime = Clock.accurateForwardProgressingMillis();
      double actualLimit = ranPermits.get() / ((endTime - startTime) / 1000);
      
      assertEquals(rateLimit, actualLimit, SLOW_MACHINE ? 75 : 50);
    } finally {
      pse.shutdownNow();
    }
  }
  
  @Test
  public void executeWithPermitsFail() {
    try {
      limiter.execute(-1, new Object(), DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.execute(1, new Object(), null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableWithPermitsFail() {
    try {
      limiter.submit(-1, new Object(), DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.submit(1, new Object(), (Runnable)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitCallableWithPermitsFail() {
    try {
      limiter.submit(-1, new Object(), new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.submit(1, new Object(), (Callable<?>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitWithNoKeyFail() {
    try {
      limiter.submit(1, null, DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.submit(1, null, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void verifyCleanupTaskTest() {
    Object key = new Object();
    limiter.execute(.1, key, DoNothingRunnable.instance());
    assertEquals(2, scheduler.advance(1000));
    assertEquals(1, limiter.getTrackedKeyCount());
    assertFalse(limiter.currentLimiters.isEmpty()); // should have item for 100 millis
    TestUtils.sleep(100);
    TestUtils.blockTillClockAdvances();
    assertEquals(1, scheduler.advance(1000));
    assertTrue(limiter.currentLimiters.isEmpty());
    assertEquals(0, limiter.getTrackedKeyCount());
  }
  
  private static class KeyedRateLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    private final int rateLimit = TEST_PROFILE == TestLoad.Stress ? 50 : 1000; 

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return new KeyedRateLimiterExecutor(schedulerFactory.makeSchedulerService(poolSize, 
                                                                           prestartIfAvailable), 
                                          rateLimit).getSubmitterExecutorForKey(new Object());
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
