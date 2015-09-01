package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTest {
  private NoThreadScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
  }
  
  @After
  public void cleanup() {
    scheduler = null;
  }
  
  private static List<TestRunnable> getRunnableList() {
    List<TestRunnable> result = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      result.add(new TestRunnable());
    }
    
    return result;
  }
  
  private static List<TestCallable> getCallableList() {
    List<TestCallable> result = new ArrayList<TestCallable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      result.add(new TestCallable());
    }
    
    return result;
  }
  
  @Test
  public void isShutdownTest() {
    assertFalse(scheduler.isShutdown());
  }
  
  @Test
  public void tickWithoutHandlerThrowsRuntimeExceptionTest() {
    RuntimeException failure = new RuntimeException();
    scheduler.execute(new TestRuntimeFailureRunnable(failure));
    
    try {
      scheduler.tick(null);
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(e == failure);
    }
  }
  
  @Test
  public void tickHandlesRuntimeExceptionTest() {
    RuntimeException failure = new RuntimeException();
    final AtomicReference<Throwable> handledException = new AtomicReference<Throwable>(null);
    scheduler.execute(new TestRuntimeFailureRunnable(failure));
    
    int runCount = scheduler.tick(new ExceptionHandler() {
      @Override
      public void handleException(Throwable thrown) {
        handledException.set(thrown);
      }
    });
    
    assertEquals(1, runCount);
    assertTrue(handledException.get() == failure);
  }
  
  @Test
  public void executeTest() {
    List<TestRunnable> runnables = getRunnableList();
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(null));
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    // verify no more run after a second tick
    assertEquals(scheduler.tick(null), 0);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
  }
  
  @Test
  public void executeInOrderTest() {
    TestRunnable lastRun = null;
    long startTime = System.currentTimeMillis();
    int testQty = 0;
    while (testQty < TEST_QTY || System.currentTimeMillis() - startTime < 100) {
      testQty++;
      final TestRunnable fLastRun = lastRun;
      lastRun = new TestRunnable() {
        @Override
        public void handleRunStart() {
          if (fLastRun != null && !fLastRun.ranOnce()) {
            fail("last run did not complete");
          }
        }
      };
      scheduler.schedule(DoNothingRunnable.instance(), 5);
      scheduler.execute(lastRun);
    }
    
    // should throw exception if any internal failures occurred
    scheduler.tick(null);
  }
  
  @Test
  public void scheduleInOrderTest() {
    TestRunnable lastRun = null;
    int testQty = 0;
    while (testQty < TEST_QTY) {
      testQty++;
      final TestRunnable fLastRun = lastRun;
      lastRun = new TestRunnable() {
        @Override
        public void handleRunStart() {
          if (fLastRun != null && !fLastRun.ranOnce()) {
            fail("last run did not complete");
          }
        }
      };
      
      scheduler.schedule(lastRun, DELAY_TIME);
    }
    
    // should throw exception if any internal failures occurred
    scheduler.tick(null);
  }
  
  @Test
  public void submitRunnableTest() {
    List<TestRunnable> runnables = getRunnableList();
    List<Future<?>> futures = new ArrayList<Future<?>>(runnables.size());
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      Future<?> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(null));
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    // verify no more run after a second tick
    assertEquals(0, scheduler.tick(null));
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    Iterator<Future<?>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      assertTrue(futureIt.next().isDone());
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    List<TestCallable> callables = getCallableList();
    List<Future<Object>> futures = new ArrayList<Future<Object>>(callables.size());
    Iterator<TestCallable> it = callables.iterator();
    while (it.hasNext()) {
      Future<Object> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(null));
    
    it = callables.iterator();
    while (it.hasNext()) {
      assertTrue(it.next().isDone());
    }

    it = callables.iterator();
    Iterator<Future<Object>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      Future<Object> future = futureIt.next();
      TestCallable tc = it.next();
      
      assertTrue(future.isDone());
      assertTrue(tc.getReturnedResult() == future.get());
    }
  }
  
  @Test
  public void scheduleRunnableTest() {
    TestRunnable tr = new TestRunnable();
    long scheduleTime = Clock.accurateForwardProgressingMillis();
    scheduler.schedule(tr, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick(null);
    }
    long runTime = Clock.accurateForwardProgressingMillis();
    
    assertEquals(1, runCount);
    
    assertTrue(tr.ranOnce());
    assertTrue((runTime - scheduleTime) >= DELAY_TIME);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = scheduler.submitScheduled(tr, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick(null);
    }
    
    assertEquals(1, runCount);
    
    assertTrue(tr.getDelayTillFirstRun() >= DELAY_TIME);
    assertTrue(future.isDone());
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    try {
      scheduler.submitScheduled((Runnable)null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.submitScheduled(new TestRunnable(), -10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable();
    ListenableFuture<?> future = scheduler.submitScheduled(tc, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick(null);
    }
    
    assertEquals(1, runCount);
    
    assertTrue(tc.getDelayTillFirstRun() >= DELAY_TIME);
    assertTrue(future.isDone());
    assertTrue(future.get() == tc.getReturnedResult());
  }
  
  @Test
  public void submitScheduledCallableFail() {
    try {
      scheduler.submitScheduled((Callable<?>)null, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.submitScheduled(new TestCallable(), -10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void scheduleWithFixedZeroDelayTest() throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    final TestRunnable[] testRunnables = new TestRunnable[TEST_QTY];
    for (int i = 0; i < TEST_QTY; i++) {
      testRunnables[i] = new TestRunnable();
    }
    
    Runnable workRunnable = new Runnable() {
      private int runIndex = -1;
      
      @Override
      public void run() {
        if (runIndex >= 0) {
          // verify they ran ahead of us
          av.assertTrue(testRunnables[runIndex].ranOnce());
        }
        
        if (++runIndex < testRunnables.length) {
          scheduler.execute(testRunnables[runIndex]);
        } else {  // we are done
          // remove task so .tick can unblock
          scheduler.remove(this);
          av.signalComplete();
        }
      }
    };
    
    scheduler.scheduleWithFixedDelay(workRunnable, 0, 0);
    
    scheduler.tick(null);
    
    av.waitForTest();
  }
  
  @Test
  public void scheduleWithFixedDelayFail() {
    try {
      scheduler.scheduleWithFixedDelay(null, 10, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), -10, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 10, -10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void scheduleAtFixedRateFail() {
    try {
      scheduler.scheduleAtFixedRate(null, 10, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleAtFixedRate(new TestRunnable(), -10, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleAtFixedRate(new TestRunnable(), 10, -10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableTest() {
    TestRunnable tr = new TestRunnable();
    
    assertFalse(scheduler.remove(tr));
    
    scheduler.execute(tr);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.submit(tr);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.submit(tr, new Object());
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.schedule(tr, DELAY_TIME);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.submitScheduled(tr, DELAY_TIME);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.submitScheduled(tr, new Object(), DELAY_TIME);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
    
    scheduler.scheduleWithFixedDelay(tr, 0, DELAY_TIME);
    assertTrue(scheduler.remove(tr));
    assertFalse(scheduler.remove(tr));
  }
  
  @Test
  public void removeRecurringRunnableTest() throws InterruptedException {
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, DELAY_TIME);
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, DELAY_TIME);
    scheduler.scheduleWithFixedDelay(initialDelay, DELAY_TIME, DELAY_TIME);
    
    assertEquals(1, scheduler.tick(null));
    
    assertEquals(1, immediateRun.getRunCount());   // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    assertEquals(1, scheduler.blockingTick(null));
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run
  }
  
  @Test
  public void removeCallableTest() throws InterruptedException {
    TestCallable immediateRun = new TestCallable();
    TestCallable delayRun = new TestCallable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.submitScheduled(immediateRun, 0);
    assertTrue(scheduler.remove(immediateRun));
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.submitScheduled(delayRun, DELAY_TIME);
    
    assertEquals(1, scheduler.blockingTick(null));
    
    assertFalse(immediateRun.isDone());
    assertTrue(delayRun.isDone());
  }
  
  @Test
  public void removeWhileRunningTest() {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() {
        assertTrue(scheduler.remove(this));
      }
    };
    
    scheduler.scheduleWithFixedDelay(tr, 0, 0);
    
    assertEquals(1, scheduler.tick(null));
    
    // should be removed for subsequent ticks
    assertEquals(0, scheduler.tick(null));
    
    assertEquals(1, tr.getRunCount());
  }
  
  @Test
  public void blockTillAvailableExecuteTest() throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    TestRunnable tickRunnable = new TestRunnable() {
      @Override
      public void handleRunStart() {
        try {
          int runCount = scheduler.blockingTick(null);  // should block
          av.assertEquals(1, runCount);
          av.signalComplete();
        } catch (InterruptedException e) {
          av.fail(e);
        }
      }
    };
    new Thread(tickRunnable).start();
    
    // should be blocked waiting for task now
    tickRunnable.blockTillStarted();
    
    TestRunnable testTask = new TestRunnable();
    scheduler.execute(testTask);
    
    testTask.blockTillFinished(); // should run without issue
    
    av.waitForTest(); // our parent thread should finish quickly
  }
  
  @Test
  public void blockTillAvailableScheduleTest() throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    final TestRunnable testTask = new TestRunnable();
    TestRunnable tickRunnable = new TestRunnable() {
      @Override
      public void handleRunStart() {
        try {
          long startTime = Clock.accurateForwardProgressingMillis();
          scheduler.schedule(testTask, DELAY_TIME);
          int runCount = scheduler.blockingTick(null);  // should block
          long finishTime = Clock.accurateForwardProgressingMillis();
          
          av.assertEquals(1, runCount);
          av.assertTrue(finishTime - startTime >= DELAY_TIME);
          av.assertTrue(testTask.ranOnce());
          av.signalComplete();
        } catch (InterruptedException e) {
          av.fail(e);
        }
      }
    };
    new Thread(tickRunnable).start();
    
    av.waitForTest();
  }
  
  @Test
  public void hasTaskReadyToRunTest() {
    assertFalse(scheduler.hasTaskReadyToRun());
    
    // schedule in the future
    scheduler.schedule(DoNothingRunnable.instance(), 1000 * 15);
    
    // still should have nothing ready to run
    assertFalse(scheduler.hasTaskReadyToRun());
    
    scheduler.execute(DoNothingRunnable.instance());
    
    // should now have tasks ready to run
    assertTrue(scheduler.hasTaskReadyToRun());
    
    scheduler.tick(null);
    
    // should no longer have anything to run
    assertFalse(scheduler.hasTaskReadyToRun());
    
    scheduler.highPriorityQueueSet
             .addScheduled(new OneTimeTaskWrapper(DoNothingRunnable.instance(), 
                                                  scheduler.highPriorityQueueSet.scheduleQueue, 
                                                  Clock.lastKnownForwardProgressingMillis()));
    
    // now should be true with scheduled task which is ready to run
    assertTrue(scheduler.hasTaskReadyToRun());
  }
  
  @Test
  public void hasTaskReadyToRunRunningTaskTest() {
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        assertFalse(scheduler.hasTaskReadyToRun());
        
        scheduler.execute(DoNothingRunnable.instance());
        assertTrue(scheduler.hasTaskReadyToRun());
        
        scheduler.remove(this);
      }
    }, 0, 1000);
    
    scheduler.tick(null);
  }
  
  @Test
  public void clearTasksTest() {
    scheduler.schedule(DoNothingRunnable.instance(), 1000 * 15);
    scheduler.execute(DoNothingRunnable.instance());
    
    scheduler.clearTasks();
    
    assertEquals(0, scheduler.highPriorityQueueSet.queueSize());
    assertEquals(0, scheduler.lowPriorityQueueSet.queueSize());
  }
}
