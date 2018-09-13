package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class KeyedRateLimiterExecutorTest extends SubmitterExecutorInterfaceTest {
  private TestableScheduler scheduler;
  private KeyedRateLimiterExecutor limiter;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    limiter = new KeyedRateLimiterExecutor(scheduler, 1, 600_000);
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
  public void executeWithPermitsReturnedDelayTest() {
    long firstDelay = limiter.execute(10, "foo", DoNothingRunnable.instance());
    assertEquals(0, firstDelay);
    
    long secondDelay = limiter.execute(1, "foo", DoNothingRunnable.instance());
    // should be incremented from first delay
    assertTrue(secondDelay > 8000);
  }
  
  @Test
  public void limitTest() throws InterruptedException, ExecutionException {
    Object key = new Object();
    int rateLimit = 200;
    final AtomicInteger ranPermits = new AtomicInteger();
    PriorityScheduler pse = new StrictPriorityScheduler(32);
    try {
      KeyedRateLimiterExecutor krls = new KeyedRateLimiterExecutor(pse, rateLimit);
      ListenableFuture<?> lastFuture = null;
      double startTime = Clock.accurateForwardProgressingMillis();
      boolean flip = true;
      for (int i = 0; i < TEST_QTY * 10; i++) {
        final int permit = (i % 4) + 1;
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

      assertEquals(rateLimit, actualLimit, SLOW_MACHINE ? 150 : 100);
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
    double permits = .1;
    Object key = new Object();
    limiter.execute(permits, key, new TestRunnable());
    assertEquals(2, scheduler.advance(KeyedRateLimiterExecutor.LIMITER_IDLE_TIMEOUT));
    assertEquals(1, limiter.getTrackedKeyCount());
    assertFalse(limiter.currentLimiters.isEmpty());
    if (TEST_PROFILE == TestLoad.Stress) {  // too slow for normal tests right now
      TestUtils.sleep((long)(KeyedRateLimiterExecutor.LIMITER_IDLE_TIMEOUT + (1000 * permits)));
      TestUtils.blockTillClockAdvances();
      assertEquals(1, scheduler.advance(KeyedRateLimiterExecutor.LIMITER_IDLE_TIMEOUT));
      assertTrue(limiter.currentLimiters.isEmpty());
      assertEquals(0, limiter.getTrackedKeyCount());
    }
  }
  
  @Test (expected = RejectedExecutionException.class)
  public void rejectDueToScheduleDelay() {
    limiter = new KeyedRateLimiterExecutor(scheduler, 1, 1000);
    limiter.execute(2000, "foo", DoNothingRunnable.instance());
    limiter.execute("foo", DoNothingRunnable.instance());
  }
  
  private static class KeyedRateLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    private final int rateLimit = TEST_PROFILE == TestLoad.Stress ? 50 : 1000; 

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      KeyedRateLimiterExecutor executor = new KeyedRateLimiterExecutor(scheduler, rateLimit, 600_000, 
                                                                       "test", true);
      
      return new SubmitterExecutor() {
        @Override
        public void execute(Runnable task) {
          executor.execute("foo", task);
        }

        @Override
        public <T> ListenableFuture<T> submit(Runnable task, T result) {
          return executor.submit("foo", task, result);
        }

        @Override
        public <T> ListenableFuture<T> submit(Callable<T> task) {
          return executor.submit("foo", task);
        }
      };
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
