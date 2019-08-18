package org.threadly.concurrent.wrapper.compatibility;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public abstract class ScheduledExecutorServiceTest extends ThreadlyTester {
  private static final int THREAD_COUNT = 1000;
  
  @BeforeClass
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  protected abstract ScheduledExecutorService makeScheduler(int poolSize);
  
  @Test
  public void shutdownTest() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      scheduler.shutdownNow();
      
      assertTrue(scheduler.isShutdown());
      
      try {
        scheduler.execute(DoNothingRunnable.instance());
        fail("Execption should have been thrown");
      } catch (RejectedExecutionException e) {
        // expected
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void isTerminatedShortTest() {
    final ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      assertFalse(scheduler.isTerminated());
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      
      tr.blockTillStarted();
      scheduler.shutdownNow();
  
      tr.blockTillFinished();
      new TestCondition(() -> scheduler.isTerminated()).blockTillTrue(1000);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void isTerminatedLongTest() {
    final ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      final int sleepTime = 100;
      
      assertFalse(scheduler.isTerminated());
      
      TestRunnable tr = new TestRunnable(sleepTime);
      scheduler.execute(tr);
      
      tr.blockTillStarted();
      scheduler.shutdownNow();
  
      tr.blockTillFinished();
      new TestCondition(() -> scheduler.isTerminated()).blockTillTrue(1000);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      assertFalse(scheduler.isTerminated());
      
      TestRunnable tr = new TestRunnable(DELAY_TIME * 2);
      long start = Clock.accurateForwardProgressingMillis();
      scheduler.execute(tr);
      
      tr.blockTillStarted();
      scheduler.shutdown();
  
      scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
      long stop = Clock.accurateForwardProgressingMillis();
      
      assertTrue(stop - start >= (DELAY_TIME * 2) - 10);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestCallable tc = new TestCallable(0);
      Future<Object> f = scheduler.submit(tc);
      
      assertTrue(f.get() == tc.getReturnedResult());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void submitWithResultTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      Object expectedResult = new Object();
      Future<Object> f = scheduler.submit(DoNothingRunnable.instance(), expectedResult);
      
      assertTrue(f.get() == expectedResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, ExecutionException, TimeoutException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestCallable tc = new TestCallable(100);
      Future<Object> f = scheduler.submit(tc);
      f.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      Future<?> f = scheduler.submit(new TestRuntimeFailureRunnable());
      
      f.get();
      fail("Exception should have been thrown");
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(2);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      final Future<?> f = scheduler.submit(btr);
      
      new Thread(new Runnable() {
        @Override
        public void run() {
          TestUtils.sleep(DELAY_TIME);
          f.cancel(true);
        }
      }).start();
      
      try {
        f.get();
        fail("exception should have been thrown");
      } catch (CancellationException e) {
        // expected
      }
    } finally {
      btr.unblock();
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleRunnableTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestRunnable tc = new TestRunnable();
      ScheduledFuture<?> f = scheduler.schedule(tc, 0, TimeUnit.MILLISECONDS);
      assertTrue(f.getDelay(TimeUnit.MILLISECONDS) <= 0);
      assertNull(f.get());
      
      assertTrue(f.isDone());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void scheduleRunnableFail() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      scheduler.schedule((Runnable)null, 10, TimeUnit.MILLISECONDS);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test (expected = NullPointerException.class)
  public void scheduleCallableFail() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      scheduler.schedule((Callable<?>)null, 10, TimeUnit.MILLISECONDS);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestCallable tc = new TestCallable(0);
      ScheduledFuture<Object> f = scheduler.schedule(tc, 0, TimeUnit.MILLISECONDS);
      assertTrue(f.getDelay(TimeUnit.MILLISECONDS) <= 0);
      assertTrue(tc.getReturnedResult() == f.get());
      
      assertTrue(f.isDone());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestCallable tcDelay = new TestCallable(0);
      ScheduledFuture<Object> delayF = scheduler.schedule(tcDelay, 20, TimeUnit.MILLISECONDS);
      long delay = delayF.getDelay(TimeUnit.MILLISECONDS);
      boolean canceled = delayF.cancel(true);
      
      assertTrue(delay <= 20);
      if (canceled) {
        assertTrue(delayF.isCancelled());
      }
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void scheduleWithFixedDelayTest() {
    recurringScheduleTest(makeScheduler(THREAD_COUNT), true);
  }

  @Test
  public void scheduleAtFixedRateTest() {
    recurringScheduleTest(makeScheduler(THREAD_COUNT), false);
  }
  
  private static void recurringScheduleTest(ScheduledExecutorService scheduler, boolean fixedDelay) {
    // schedule a task first in case there are any initial startup actions which may be slow
    if (fixedDelay) {
      scheduler.scheduleWithFixedDelay(DoNothingRunnable.instance(), 0, (int)(DELAY_TIME * 2.5), 
                                       TimeUnit.MILLISECONDS);
    } else {
      scheduler.scheduleAtFixedRate(DoNothingRunnable.instance(), 0, (int)(DELAY_TIME * 2.5), 
                                    TimeUnit.MILLISECONDS);
    }
    
    List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = new TestRunnable(fixedDelay ? 0 : DELAY_TIME);
      if (fixedDelay) {
        scheduler.scheduleWithFixedDelay(tr, 0, DELAY_TIME, 
                                         TimeUnit.MILLISECONDS);
      } else {
        scheduler.scheduleAtFixedRate(tr, 0, DELAY_TIME, 
                                      TimeUnit.MILLISECONDS);
      }
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      // verify runnable
      TestRunnable tr = it.next();
      
      tr.blockTillFinished((DELAY_TIME * (CYCLE_COUNT - 1)) + 5000, CYCLE_COUNT);
      long executionDelay = tr.getDelayTillRun(CYCLE_COUNT);
      assertTrue(executionDelay >= DELAY_TIME * (CYCLE_COUNT - 1));
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (DELAY_TIME * (CYCLE_COUNT - 1)) + (SLOW_MACHINE ? 5000 : 1000));
    }
  }
  
  @Test
  public void scheduleWithFixedDelayExceptionTest() {
    recurringExceptionTest(false);
  }

  @Test
  public void scheduleAtFixedRateExceptionTest() {
    recurringExceptionTest(true);
  }
  
  private void recurringExceptionTest(boolean atFixedRate) {
    ScheduledExecutorService scheduler = makeScheduler(2);
    try {
      final int runCountTillException = 4;
      
      TestRunnable tr = new TestRunnable() {
        @Override
        public void handleRunFinish() {
          if (this.getRunCount() >= runCountTillException) {
            throw new StackSuppressedRuntimeException();
          }
        }
      };
      
      if (atFixedRate) {
        scheduler.scheduleAtFixedRate(tr, 0, 1, TimeUnit.MILLISECONDS);
      } else {
        scheduler.scheduleWithFixedDelay(tr, 0, 1, TimeUnit.MILLISECONDS);
      }
      
      // block till we have run enough times to throw exception
      tr.blockTillFinished(1000 * 10, runCountTillException);
      
      // wait a little extra to give time for additional runs if possible
      TestUtils.sleep(DELAY_TIME);
      
      assertEquals(runCountTillException, tr.getRunCount());
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void scheduleWithFixedDelayFail() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      try {
        scheduler.scheduleWithFixedDelay(null, 0, 10, 
                                         TimeUnit.MILLISECONDS);
        fail("Exception should have been thrown");
      } catch (NullPointerException e) {
        // expected
      }
      try {
        scheduler.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 0, 
                                         TimeUnit.MILLISECONDS);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void scheduleAtFixedRateConcurrentTest() {
    ScheduledExecutorService scheduler = makeScheduler(2);
    try {
      final int periodInMillis = DELAY_TIME;
      final int runnableSleepTime = DELAY_TIME * 2;
      
      TestRunnable tr = new TestRunnable(runnableSleepTime);
      
      scheduler.scheduleAtFixedRate(tr, 0, periodInMillis, TimeUnit.MILLISECONDS);
      
      // block till we have run enough times to throw exception
      tr.blockTillFinished(1000 * 10, CYCLE_COUNT);
      
      // let all runnables finish readily
      tr.setRunDelayInMillis(0);
      
      // wait a little extra to give time for additional runs
      TestUtils.sleep(periodInMillis * 2);
      
      assertFalse(tr.ranConcurrently());
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void scheduleAtFixedRateFail() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      try {
        scheduler.scheduleAtFixedRate(null, 0, 10, TimeUnit.MILLISECONDS);
        fail("Exception should have been thrown");
      } catch (NullPointerException e) {
        // expected
      }
      try {
        scheduler.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 0, TimeUnit.MILLISECONDS);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void scheduleAtFixedRateFutureCancelRemovalTest() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    ScheduledFuture<?> sf = scheduler.scheduleAtFixedRate(DoNothingRunnable.instance(), 
                                                          1000, 1000, TimeUnit.MILLISECONDS);
    sf.cancel(false);

    // task should no longer be queued with scheduler
    assertEquals(0, scheduler.shutdownNow().size());
  }
  
  @Test
  public void scheduleWithFixedDelayFutureCancelRemovalTest() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    ScheduledFuture<?> sf = scheduler.scheduleWithFixedDelay(DoNothingRunnable.instance(), 
                                                             1000, 1000, TimeUnit.MILLISECONDS);
    sf.cancel(false);
    
    // task should no longer be queued with scheduler
    assertEquals(0, scheduler.shutdownNow().size());
  }

  @Test
  public void invokeAllTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      List<TestCallable> toInvoke = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        toInvoke.add(new TestCallable(0));
      }
      List<Future<Object>> result = scheduler.invokeAll(toInvoke);
      
      assertEquals(toInvoke.size(), result.size());
      
      Iterator<TestCallable> it = toInvoke.iterator();
      Iterator<Future<Object>> resultIt = result.iterator();
      while (it.hasNext()) {
        assertTrue(resultIt.next().get() == it.next().getReturnedResult());
      }
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void invokeAllExceptionTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      int exceptionCallableIndex = TEST_QTY / 2;
      
      List<TestCallable> toInvoke = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestCallable tc;
        if (i == exceptionCallableIndex) {
          tc = new TestCallable(0) {
            @Override
            protected void handleCallStart() {
              throw new StackSuppressedRuntimeException();
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
      for (int i = 0; i < TEST_QTY; i++) {
        if (i != exceptionCallableIndex) {
          assertTrue(resultIt.next().get() == it.next().getReturnedResult());
        } else {
          // skip fail entry
          resultIt.next();
          it.next();
        }
      }
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void invokeAllTimeoutTest() throws InterruptedException {
    ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      int runTime = 1000 * 10;
      int timeoutTime = 5;
      
      List<TestCallable> toInvoke = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        toInvoke.add(new TestCallable(runTime));
      }
      
      long startTime = Clock.accurateForwardProgressingMillis();
      List<Future<Object>> result = scheduler.invokeAll(toInvoke, timeoutTime, TimeUnit.MILLISECONDS);
      long endTime = Clock.accurateForwardProgressingMillis();
      
      assertEquals(toInvoke.size(), result.size());
      
      assertTrue(endTime - startTime >= timeoutTime);
      assertTrue(endTime - startTime < timeoutTime + (SLOW_MACHINE ? 5000 : 500));
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test (expected = NullPointerException.class)
  public void invokeAllFail() throws InterruptedException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      List<TestCallable> toInvoke = new ArrayList<>(2);
      toInvoke.add(new TestCallable(0));
      toInvoke.add(null);
      scheduler.invokeAll(toInvoke);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void invokeAnyTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(THREAD_COUNT);
    try {
      List<TestCallable> toInvoke = new ArrayList<>(TEST_QTY);
      Object expectedResult = null;
      for (int i = 0; i < TEST_QTY; i++) {
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
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test (expected = TimeoutException.class)
  public void invokeAnyTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      int runTime = 1000 * 10;
      int timeoutTime = 5;
      
      List<TestCallable> toInvoke = new ArrayList<>(1);
  
      toInvoke.add(new TestCallable(runTime));
      
      scheduler.invokeAny(toInvoke, timeoutTime, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void invokeAnyFirstTaskExceptionTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      TestCallable tc = new TestCallable();
      List<Callable<Object>> toInvoke = new ArrayList<>(2);
      toInvoke.add(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          throw new Exception();
        }
      });
      toInvoke.add(tc);
      
      Object result = scheduler.invokeAny(toInvoke);
      assertTrue(result == tc.getReturnedResult());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test (expected = ExecutionException.class)
  public void invokeAnyAllTasksExceptionTest() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      Callable<Object> failureCallable = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          throw new Exception();
        }
      };
      List<Callable<Object>> toInvoke = new ArrayList<>(2);
      toInvoke.add(failureCallable);
      toInvoke.add(failureCallable);
      
      scheduler.invokeAny(toInvoke);
      fail("Execution exception should have thrown");
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void invokeAnyFail() throws InterruptedException, ExecutionException {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      try {
        List<TestCallable> toInvoke = new ArrayList<>(2);
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
      try {
        scheduler.invokeAny(Collections.emptyList());
        fail("Exception should have thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
}
