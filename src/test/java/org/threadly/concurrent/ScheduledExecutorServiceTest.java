package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ScheduledExecutorServiceTest {
  public static void isTerminatedShortTest(ScheduledExecutorService scheduler) {
    assertFalse(scheduler.isTerminated());
    
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr);
    
    tr.blockTillStarted();
    scheduler.shutdownNow();

    tr.blockTillFinished();
    TestUtils.sleep(100);
    assertTrue(scheduler.isTerminated());
  }
  
  public static void isTerminatedLongTest(ScheduledExecutorService scheduler) {
    final int sleepTime = 100;
    
    assertFalse(scheduler.isTerminated());
    
    TestRunnable tr = new TestRunnable(sleepTime);
    scheduler.execute(tr);
    
    tr.blockTillStarted();
    scheduler.shutdownNow();

    tr.blockTillFinished();
    TestUtils.sleep(100);
    assertTrue(scheduler.isTerminated());
  }
  
  public static void awaitTerminationTest(ScheduledExecutorService scheduler) throws InterruptedException {
    final int sleepTime = 200;
    
    assertFalse(scheduler.isTerminated());
    
    TestRunnable tr = new TestRunnable(sleepTime);
    long start = System.currentTimeMillis();
    scheduler.execute(tr);
    
    tr.blockTillStarted();
    scheduler.shutdown();

    scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
    long stop = System.currentTimeMillis();
    
    assertTrue(stop - start >= sleepTime - 10);
  }
  
  public static void submitCallableTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                   ExecutionException {
    TestCallable tc = new TestCallable(0);
    Future<Object> f = scheduler.submit(tc);
    
    assertTrue(f.get() == tc.getReturnedResult());
  }
  
  public static void submitWithResultTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException {
    Object expectedResult = new Object();
    Future<Object> f = scheduler.submit(new TestRunnable(), expectedResult);
    
    assertTrue(f.get() == expectedResult);
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
    Future<?> f = scheduler.submit(new TestRuntimeFailureRunnable());
    
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
    assertTrue(tc.getReturnedResult() == f.get());
    
    assertTrue(f.isDone());
  }
  
  public static void scheduleCallableCancelTest(ScheduledExecutorService scheduler) {
    TestCallable tcDelay = new TestCallable(0);
    ScheduledFuture<Object> delayF = scheduler.schedule(tcDelay, 20, TimeUnit.MILLISECONDS);
    long delay = delayF.getDelay(TimeUnit.MILLISECONDS);
    boolean canceled = delayF.cancel(true);
    
    assertTrue(delay <= 20);
    if (canceled) {
      assertTrue(delayF.isCancelled());
    }
  }
  
  private static void recurringScheduleTest(ScheduledExecutorService scheduler, boolean fixedDelay) {
    final int runnableCount = 10;
    final int recurringDelay = 50;
    final int waitCount = 2;
    
    // schedule a task first in case there are any initial startup actions which may be slow
    if (fixedDelay) {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 0, 1000 * 10, TimeUnit.MILLISECONDS);
    } else {
      scheduler.scheduleAtFixedRate(new TestRunnable(), 0, 1000 * 10, TimeUnit.MILLISECONDS);
    }
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      if (fixedDelay) {
        scheduler.scheduleWithFixedDelay(tr, 0, recurringDelay, 
                                         TimeUnit.MILLISECONDS);
      } else {
        scheduler.scheduleAtFixedRate(tr, 0, recurringDelay, 
                                      TimeUnit.MILLISECONDS);
      }
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      // verify runnable
      TestRunnable tr = it.next();
      
      tr.blockTillFinished((runnableCount * (recurringDelay * waitCount)) + 2000, waitCount);
      long executionDelay = tr.getDelayTillRun(waitCount);
      assertTrue(executionDelay >= recurringDelay * (waitCount - 1));
      
      if (fixedDelay) {
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (recurringDelay * (waitCount - 1)) + 2000);
      }
    }
  }
  
  public static void scheduleWithFixedDelayTest(ScheduledExecutorService scheduler) {
    recurringScheduleTest(scheduler, true);
  }
  
  public static void scheduleWithFixedDelayFail(ScheduledExecutorService scheduler) {
    scheduler.scheduleWithFixedDelay(null, 0, 10, 
                                     TimeUnit.MILLISECONDS);
    fail("Exception should have been thrown");
  }
  
  public static void scheduleAtFixedRateTest(ScheduledExecutorService scheduler) {
    recurringScheduleTest(scheduler, true);
  }
  
  public static void scheduleAtFixedRateConcurrentTest(ScheduledExecutorService scheduler) {
    final int periodInMillis = 5;
    final int runnableSleepTime = periodInMillis * 2;
    final int testRunCount = 4;
    
    TestRunnable tr = new TestRunnable(runnableSleepTime);
    
    scheduler.scheduleAtFixedRate(tr, 0, periodInMillis, TimeUnit.MILLISECONDS);
    
    // block till we have run enough times to throw exception
    tr.blockTillFinished(1000 * 10, testRunCount);
    
    // let all runnables finish readily
    tr.setRunDelayInMillis(0);
    
    // wait a little extra to give time for additional runs
    TestUtils.sleep(periodInMillis * 2);
    
    assertFalse(tr.ranConcurrently());
  }
  
  public static void scheduleAtFixedRateExceptionTest(ScheduledExecutorService scheduler) {
    final int periodInMillis = 10;
    final int runCountTillException = 4;
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        if (this.getRunCount() >= runCountTillException) {
          throw new RuntimeException();
        }
      }
    };
    
    scheduler.scheduleAtFixedRate(tr, 0, periodInMillis, TimeUnit.MILLISECONDS);
    
    // block till we have run enough times to throw exception
    tr.blockTillFinished(1000 * 10, runCountTillException);
    
    // wait a little extra to give time for additional runs if possible
    TestUtils.sleep(periodInMillis * 2);
    
    assertEquals(runCountTillException, tr.getRunCount());
  }

  public static void scheduleAtFixedRateFail(ScheduledExecutorService scheduler) {
    try {
      scheduler.scheduleAtFixedRate(null, 0, 10, 
                                    TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (NullPointerException e) {
      // expected
    }
    try {
      scheduler.scheduleAtFixedRate(new TestRunnable(), 10, 0, 
                                    TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public static void invokeAllTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                              ExecutionException {
    int callableQty = 10;
    
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(callableQty);
    for (int i = 0; i < callableQty; i++) {
      toInvoke.add(new TestCallable(0));
    }
    List<Future<Object>> result = scheduler.invokeAll(toInvoke);
    
    assertEquals(toInvoke.size(), result.size());
    
    Iterator<TestCallable> it = toInvoke.iterator();
    Iterator<Future<Object>> resultIt = result.iterator();
    while (it.hasNext()) {
      assertTrue(resultIt.next().get() == it.next().getReturnedResult());
    }
  }
  
  public static void invokeAllExceptionTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                       ExecutionException {
    int callableQty = 10;
    int exceptionCallableIndex = 5;
    
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(callableQty);
    for (int i = 0; i < callableQty; i++) {
      TestCallable tc;
      if (i == exceptionCallableIndex) {
        tc = new TestCallable(0) {
          @Override
          protected void handleCallStart() {
            throw new RuntimeException();
          }
        };
      } else {
        tc = new TestCallable(0);
      }
      toInvoke.add(tc);
    }
    List<Future<Object>> result = scheduler.invokeAll(toInvoke);
    
    assertEquals(toInvoke.size(), result.size());
    
    Iterator<TestCallable> it = toInvoke.iterator();
    Iterator<Future<Object>> resultIt = result.iterator();
    for (int i = 0; i < callableQty; i++) {
      if (i != exceptionCallableIndex) {
        assertTrue(resultIt.next().get() == it.next().getReturnedResult());
      } else {
        // skip fail entry
        resultIt.next();
        it.next();
      }
    }
  }
  
  public static void invokeAllTimeoutTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException {
    int runTime = 1000 * 10;
    int callableQty = 10;
    int timeoutTime = 10;
    
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(callableQty);
    for (int i = 0; i < callableQty; i++) {
      toInvoke.add(new TestCallable(runTime));
    }
    
    long startTime = System.currentTimeMillis();
    List<Future<Object>> result = scheduler.invokeAll(toInvoke, timeoutTime, TimeUnit.MILLISECONDS);
    long endTime = System.currentTimeMillis();
    
    assertEquals(toInvoke.size(), result.size());
    
    assertTrue(endTime - startTime >= timeoutTime);
    assertTrue(endTime - startTime < timeoutTime + 250);
  }
  
  public static void invokeAllFail(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                              ExecutionException {
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(2);
    toInvoke.add(new TestCallable(0));
    toInvoke.add(null);
    scheduler.invokeAll(toInvoke);
  }
  
  public static void invokeAnyTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                              ExecutionException {
    int callableQty = 10;
    
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(callableQty);
    Object expectedResult = null;
    for (int i = 0; i < callableQty; i++) {
      TestCallable tc;
      if (i == 0) {
        tc = new TestCallable(0);
        expectedResult = tc.getReturnedResult();
      } else {
        tc = new TestCallable(1000 + i - 1);
      }
      toInvoke.add(tc);
    }
    Object result = scheduler.invokeAny(toInvoke);
    
    assertNotNull(result);
    
    assertTrue(result == expectedResult);
  }
  
  public static void invokeAnyTimeoutTest(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                                     ExecutionException, 
                                                                                     TimeoutException {
    int runTime = 1000 * 10;
    int callableQty = 10;
    int timeoutTime = 10;
    
    List<TestCallable> toInvoke = new ArrayList<TestCallable>(callableQty);

    toInvoke.add(new TestCallable(runTime));
    
    scheduler.invokeAny(toInvoke, timeoutTime, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }
  
  public static void invokeAnyFail(ScheduledExecutorService scheduler) throws InterruptedException, 
                                                                              ExecutionException {
    try {
      List<TestCallable> toInvoke = new ArrayList<TestCallable>(2);
      toInvoke.add(new TestCallable(0));
      toInvoke.add(null);
      scheduler.invokeAny(toInvoke);
      fail("Exception should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
    try {
      scheduler.invokeAny(null);
      fail("Exception should have thrown");
    } catch (NullPointerException e) {
      // expected
    }
  }
}
