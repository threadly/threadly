package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutorInterface;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class RateLimiterExecutorTest extends SubmitterExecutorInterfaceTest {
  private RateLimiterExecutor limiter;
  private TestableScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
    limiter = new RateLimiterExecutor(scheduler, 1);
  }
  
  @After
  public void cleanupDown() {
    scheduler = null;
    limiter = null;
  }

  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new RateLimiterFactory();
  }
  
  @SuppressWarnings("unused")
  @Test
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
    limiter.execute(2, DoNothingRunnable.instance());
    f = limiter.getFutureTillDelay(0);
    assertFalse(f.isDone());
    
    scheduler.advance(2000);
    assertTrue(f.isDone());
  }
  
  @Test
  public void limitTest() throws InterruptedException, ExecutionException {
    int rateLimit = 100;
    final AtomicInteger ranPermits = new AtomicInteger();
    PriorityScheduler pse = new StrictPriorityScheduler(32);
    try {
      RateLimiterExecutor rls = new RateLimiterExecutor(pse, rateLimit);
      rls.execute(rateLimit + 1, DoNothingRunnable.instance());
      ListenableFuture<?> lastFuture = null;
      double startTime = Clock.accurateForwardProgressingMillis();
      boolean flip = true;
      for (int i = 0; i < TEST_QTY * 2; i++) {
        final int permit = 10;
        if (flip) {
          lastFuture = rls.submit(permit, new Runnable() {
            @Override
            public void run() {
              ranPermits.addAndGet(permit);
            }
          });
          flip = false;
        } else {
          lastFuture = rls.submit(permit, new Callable<Void>() {
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
      
      assertEquals(rateLimit, actualLimit, 50);
    } finally {
      pse.shutdownNow();
    }
  }
  
  @Test
  public void executeWithPermitsFail() {
    try {
      limiter.execute(-1, new TestRunnable());
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
      limiter.submit(-1, new TestRunnable());
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
  
  private static class RateLimiterFactory implements SubmitterExecutorFactory {
    private final int rateLimit = TEST_PROFILE == TestLoad.Stress ? 50 : 1000; 
    private final List<PriorityScheduler> executors = new LinkedList<PriorityScheduler>();

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        executor.prestartAllThreads();
      }
      executors.add(executor);
      
      return new RateLimiterExecutor(executor, rateLimit);
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
}
