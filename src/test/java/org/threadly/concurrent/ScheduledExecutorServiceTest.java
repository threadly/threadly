package org.threadly.concurrent;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtil;

@SuppressWarnings("javadoc")
public class ScheduledExecutorServiceTest {
  public static void isTerminatedTest(ScheduledExecutorService scheduler) {
    assertFalse(scheduler.isTerminated());
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() throws InterruptedException {
        Thread.sleep(50);
      }
    };
    scheduler.execute(tr);
    
    TestUtil.sleep(10);
    scheduler.shutdown();

    tr.blockTillRun();
    TestUtil.sleep(100);
    assertTrue(scheduler.isTerminated());
  }
  
  public static void awaitTerminationTest(ScheduledExecutorService scheduler) throws InterruptedException {
    final int sleepTime = 50;
    
    assertFalse(scheduler.isTerminated());
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() throws InterruptedException {
        Thread.sleep(sleepTime);
      }
    };
    long start = System.currentTimeMillis();
    scheduler.execute(tr);
    
    TestUtil.sleep(10);
    scheduler.shutdown();

    scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
    long stop = System.currentTimeMillis();
    
    assertTrue(stop - start >= sleepTime - 10);
  }
  
  public static void submitCallableTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                   ExecutionException {
    TestCallable tc = new TestCallable(0);
    Future<Object> f = scheduler.submit(tc);
    
    assertTrue(f.get() == tc.result);
  }
  
  public static void futureGetTimeoutFail(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException, 
                                                                                     TimeoutException {
    TestCallable tc = new TestCallable(100);
    Future<Object> f = scheduler.submit(tc);
    f.get(1, TimeUnit.MILLISECONDS);
    fail("Exception should have been thrown");
  }
  
  public static void futureGetExecutionFail(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                       ExecutionException {
    Future<?> f = scheduler.submit(new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException("fail");
      }
    });
    
    f.get();
    fail("Exception should have been thrown");
  }
  
  public static void futureCancelTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                 ExecutionException {
    TestCallable tc = new TestCallable(500);
    final Future<Object> f = scheduler.submit(tc);
    
    scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        f.cancel(true);
      }
    }, 10, TimeUnit.MILLISECONDS);
    
    try {
      f.get();
      fail("exception should have been thrown");
    } catch (CancellationException e) {
      // expected
    }
    
    assertTrue(f.isCancelled());
  }
  
  public static void scheduleRunnableTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException {
    TestRunnable tc = new TestRunnable();
    ScheduledFuture<?> f = scheduler.schedule(tc, 0, TimeUnit.MILLISECONDS);
    assertTrue(f.getDelay(TimeUnit.MILLISECONDS) <= 0);
    assertNull(f.get());
    
    assertTrue(f.isDone());
  }
  
  public static void scheduleCallableTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException {
    TestCallable tc = new TestCallable(0);
    ScheduledFuture<Object> f = scheduler.schedule(tc, 0, TimeUnit.MILLISECONDS);
    assertTrue(f.getDelay(TimeUnit.MILLISECONDS) <= 0);
    assertTrue(tc.result == f.get());
    
    assertTrue(f.isDone());
  }
  
  public static void scheduleCallableCancelTest(ScheduledExecutorService scheduler) {
    TestCallable tcDelay = new TestCallable(0);
    ScheduledFuture<Object> delayF = scheduler.schedule(tcDelay, 20, TimeUnit.MILLISECONDS);
    long delay = delayF.getDelay(TimeUnit.MILLISECONDS);
    delayF.cancel(true);
    
    assertTrue(delay <= 20);
    assertTrue(delayF.isCancelled());
  }
  
  public static void scheduleWithFixedDelayTest(ScheduledExecutorService scheduler) {
    int runnableCount = 10;
    int recurringDelay = 50;
    int waitCount = 3;
    
    long startTime = System.currentTimeMillis();
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, recurringDelay, 
                                       TimeUnit.MILLISECONDS);
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      // verify runnable
      TestRunnable tr = it.next();
      
      tr.blockTillRun(runnableCount * recurringDelay + 500, waitCount);
      long executionDelay = tr.getDelayTillRun(waitCount);
      assertTrue(executionDelay >= recurringDelay * (waitCount - 1));
      
      assertTrue(executionDelay <= ((recurringDelay * (waitCount - 1)) + 500));
      int expectedRunCount = (int)((System.currentTimeMillis() - startTime) / recurringDelay);
      assertTrue(tr.getRunCount() >= expectedRunCount - 2);
      assertTrue(tr.getRunCount() <= expectedRunCount + 2);
    }
  }
  
  public static void scheduleWithFixedDelayFail(ScheduledExecutorService scheduler) {
    try {
      scheduler.scheduleWithFixedDelay(null, 0, 10, 
                                       TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (NullPointerException e) {
      // expected
    }
  }
  
  private static class TestCallable extends TestCondition 
                                    implements Callable<Object> {
    private final long runTime;
    private final Object result;
    private volatile boolean done;
    
    private TestCallable(long runTime) {
      this.runTime = runTime;
      result = new Object();
      done = false;
    }

    @Override
    public Object call() {
      TestUtil.sleep(runTime);
      
      done = true;
      
      return result;
    }

    @Override
    public boolean get() {
      return done;
    }
  }
}
