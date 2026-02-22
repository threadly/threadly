package org.threadly.concurrent.wrapper.limiter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class RateLimiterExecutorTest extends SubmitterExecutorInterfaceTest {
  private RateLimiterExecutor limiter;
  private TestableScheduler scheduler;
  
  @BeforeEach
  public void setup() {
    scheduler = new TestableScheduler();
    limiter = new RateLimiterExecutor(scheduler, 1);
  }
  
  @AfterEach
  public void cleanupDown() {
    scheduler = null;
    limiter = null;
  }

  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new RateLimiterFactory();
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new RateLimiterExecutor(null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new RateLimiterExecutor(scheduler, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getCurrentMinimumDelayTest() {
    assertEquals(0, limiter.getMinimumDelay());
    
    limiter.execute(10, DoNothingRunnable.instance());
    int delay = limiter.getMinimumDelay();
    assertEquals(10000, delay, 1000);
    
    limiter.execute(10, DoNothingRunnable.instance());
    delay = limiter.getMinimumDelay();
    assertEquals(20000, delay, 1000);
  }
  
  @Test
  public void getFutureTillDelayTest() {
    // verify that an empty limiter returns a finished future
    ListenableFuture<?> f = limiter.getFutureTillDelay(0);
    assertTrue(f.isDone());
    
    // verify a it works if the limiter has waiting tasks
    limiter.execute(1, DoNothingRunnable.instance());
    f = limiter.getFutureTillDelay(0);
    assertFalse(f.isDone());
    
    scheduler.advance(1000);
    assertTrue(f.isDone());
  }
  
  @Test
  public void executeWithPermitsReturnedDelayTest() {
    long firstDelay = limiter.execute(10, DoNothingRunnable.instance());
    assertEquals(0, firstDelay);
    
    long secondDelay = limiter.execute(1, DoNothingRunnable.instance());
    // should be incremented from first delay
    assertTrue(secondDelay > 8000);
  }
  
  @Test
  public void limitTest() throws InterruptedException, ExecutionException {
    int rateLimit = 200;
    final AtomicInteger ranPermits = new AtomicInteger();
    PriorityScheduler pse = new StrictPriorityScheduler(32);
    try {
      RateLimiterExecutor rls = new RateLimiterExecutor(pse, rateLimit);
      ListenableFuture<?> lastFuture = null;
      double startTime = Clock.accurateForwardProgressingMillis();
      for (int i = 0; i < TEST_QTY * 10; i++) {
        final int permit = (i % 4) + 1;
        if (i % 2 == 0) {
          lastFuture = rls.submit(permit, new Runnable() {
            @Override
            public void run() {
              ranPermits.addAndGet(permit);
            }
          });
        } else {
          lastFuture = rls.submit(permit, new Callable<Void>() {
            @Override
            public Void call() {
              ranPermits.addAndGet(permit);
              return null;
            }
          });
        }
      }
      lastFuture.get();
      long endTime = Clock.accurateForwardProgressingMillis();
      double actualLimit = ranPermits.get() / ((endTime - startTime) / 1000.);
      
      assertEquals(rateLimit, actualLimit, SLOW_MACHINE ? 150 : 100);
    } finally {
      pse.shutdownNow();
    }
  }
  
  @Test
  public void executeWithPermitsFail() {
    try {
      limiter.execute(-1, DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.execute(1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableWithPermitsFail() {
    try {
      limiter.submit(-1, DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.submit(1, (Runnable)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitCallableWithPermitsFail() {
    try {
      limiter.submit(-1, new TestCallable());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      limiter.submit(1, (Callable<?>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void rejectDueToScheduleDelay() {
      assertThrows(RejectedExecutionException.class, () -> {
      limiter = new RateLimiterExecutor(scheduler, 1, 1000);
      limiter.execute(2000, DoNothingRunnable.instance());
      limiter.execute(DoNothingRunnable.instance());
      });
  }
  
  private static class RateLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    private final int rateLimit = TEST_PROFILE == TestLoad.Stress ? 50 : 1000; 

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return new RateLimiterExecutor(schedulerFactory.makeSchedulerService(poolSize, 
                                                                           prestartIfAvailable), 
                                     rateLimit);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
