package org.threadly.test.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public class TestableSchedulerTest {
  protected TestableScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new TestableScheduler();
  }
  
  @After
  public void cleanup() {
    scheduler = null;
  }
  
  private static List<TestRunnable> getRunnableList() {
    List<TestRunnable> result = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      result.add(new TestRunnable());
    }
    
    return result;
  }
  
  private static List<TestCallable> getCallableList() {
    List<TestCallable> result = new ArrayList<>(TEST_QTY);
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
  public void lastTickTimeTest() {
    long now = Clock.lastKnownTimeMillis();
    
    scheduler.tick(now + 1000);
    
    assertEquals(now + 1000, scheduler.getLastTickTime());
  }
  
  @Test
  public void advanceWithoutHandlerThrowsRuntimeExceptionTest() {
    RuntimeException failure = new SuppressedStackRuntimeException();
    scheduler.execute(new TestRuntimeFailureRunnable(failure));
    
    try {
      scheduler.advance(10);
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(e == failure);
    }
  }
  
  @Test
  public void advanceHandlesRuntimeExceptionTest() {
    RuntimeException failure = new SuppressedStackRuntimeException();
    final AtomicReference<Throwable> handledException = new AtomicReference<>(null);
    scheduler.execute(new TestRuntimeFailureRunnable(failure));
    
    int runCount = scheduler.advance(10, new ExceptionHandler() {
      @Override
      public void handleException(Throwable thrown) {
        handledException.set(thrown);
      }
    });
    
    assertEquals(1, runCount);
    assertTrue(handledException.get() == failure);
  }
  
  @Test
  public void tickWithoutHandlerThrowsRuntimeExceptionTest() {
    RuntimeException failure = new SuppressedStackRuntimeException();
    scheduler.execute(new TestRuntimeFailureRunnable(failure));
    
    try {
      scheduler.tick();
      fail("Exception should have thrown");
    } catch (Exception e) {
      assertTrue(e == failure);
    }
  }
  
  @Test
  public void tickHandlesRuntimeExceptionTest() {
    RuntimeException failure = new SuppressedStackRuntimeException();
    final AtomicReference<Throwable> handledException = new AtomicReference<>(null);
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
  public void advanceTest() {
    long now = scheduler.getLastTickTime();
    scheduler.advance(1000);
    
    assertEquals(now + 1000, scheduler.getLastTickTime());
  }
  
  @Test
  public void advanceThenTickTest() {
    scheduler.execute(DoNothingRunnable.instance());
    scheduler.advance(1000 * 10);
    
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr);
    
    assertEquals(1, scheduler.tick());
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void executeTest() {
    List<TestRunnable> runnables = getRunnableList();
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(Clock.lastKnownTimeMillis()));
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    // verify no more run after a second tick
    assertEquals(scheduler.tick(), 0);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
  }
  
  @Test
  public void submitRunnableTest() {
    List<TestRunnable> runnables = getRunnableList();
    List<Future<?>> futures = new ArrayList<>(runnables.size());
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      Future<?> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(Clock.lastKnownTimeMillis()));
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(1, it.next().getRunCount());
    }
    
    // verify no more run after a second tick
    assertEquals(0, scheduler.tick());
    
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
    List<Future<Object>> futures = new ArrayList<>(callables.size());
    Iterator<TestCallable> it = callables.iterator();
    while (it.hasNext()) {
      Future<Object> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick(Clock.lastKnownTimeMillis()));
    
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
    long scheduleDelay = 1000 * 10;
    
    TestRunnable executeRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    scheduler.schedule(scheduleRun, scheduleDelay);
    scheduler.execute(executeRun);

    assertEquals(1, scheduler.tick());

    assertEquals(1, executeRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, scheduler.advance(scheduleDelay));
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(scheduler.advance(scheduleDelay), 0); // should not execute anything
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable submitRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    Future<?> future = scheduler.submit(submitRun);
    assertNotNull(future);
    future = scheduler.submitScheduled(scheduleRun, scheduleDelay);
    assertNotNull(future);

    assertEquals(1, scheduler.tick());

    assertEquals(1, submitRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, scheduler.advance(scheduleDelay));
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(0, scheduler.advance(scheduleDelay)); // should not execute anything
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
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
  public void submitScheduledCallableTest() {
    long scheduleDelay = 1000 * 10;
    
    TestCallable submitRun = new TestCallable();
    TestCallable scheduleRun = new TestCallable();
    
    Future<?> future = scheduler.submit(submitRun);
    assertNotNull(future);
    future = scheduler.submitScheduled(scheduleRun, scheduleDelay);
    assertNotNull(future);

    assertEquals(1, scheduler.tick());

    assertTrue(submitRun.isDone());   // should have run
    assertFalse(scheduleRun.isDone());  // should NOT have run yet
    
    assertEquals(1, scheduler.advance(scheduleDelay));
    
    assertTrue(scheduleRun.isDone());  // should have run
    
    assertEquals(0, scheduler.advance(scheduleDelay)); // should not execute anything
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
  public void scheduleWithFixedDelayTest() {
    scheduleRecurringTest(true);
  }
  
  private void scheduleRecurringTest(boolean fixedDelay) {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    if (fixedDelay) {
      scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
      scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);
    } else {
      scheduler.scheduleAtFixedRate(immediateRun, 0, delay);
      scheduler.scheduleAtFixedRate(initialDelay, delay, delay);
    }

    assertEquals(1, scheduler.tick());
    
    assertEquals(1, immediateRun.getRunCount());  // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet

    assertEquals(2, scheduler.advance(delay));
    
    assertEquals(2, immediateRun.getRunCount());  // should have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run for the first time
    
    assertEquals(2, scheduler.advance(delay));
    
    assertEquals(3, immediateRun.getRunCount());  // should have run again
    assertEquals(2, initialDelay.getRunCount());  // should have run again
    
    assertEquals(0, scheduler.tick(scheduler.getLastTickTime())); // should not execute anything
    
    assertEquals(3, immediateRun.getRunCount());  // should NOT have run again
    assertEquals(2, initialDelay.getRunCount());  // should NOT have run again
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
  public void scheduleAtFixedRateTest() {
    scheduleRecurringTest(false);
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
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);
    
    assertEquals(1, scheduler.tick());
    
    assertEquals(1, immediateRun.getRunCount());   // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    assertEquals(1, scheduler.advance(delay));
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run
    
    assertEquals(0, scheduler.tick(scheduler.getLastTickTime())); // should not execute anything
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run
    assertEquals(1, initialDelay.getRunCount());  // should NOT have run
  }
  
  @Test
  public void removeCallableTest() {
    long delay = 1000 * 10;
    
    TestCallable immediateRun = new TestCallable();
    TestCallable delayRun = new TestCallable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.submitScheduled(immediateRun, 0);
    assertTrue(scheduler.remove(immediateRun));
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.submitScheduled(delayRun, delay);
    
    assertEquals(0, scheduler.tick());
    
    // neither should run yet
    assertFalse(immediateRun.isDone());
    assertFalse(delayRun.isDone());
    
    scheduler.submitScheduled(immediateRun, 0);
    
    assertEquals(2, scheduler.advance(delay));
    
    // both should run now
    assertTrue(immediateRun.isDone());
    assertTrue(delayRun.isDone());
    
    // neither should be in scheduler any more
    assertFalse(scheduler.remove(immediateRun));
    assertFalse(scheduler.remove(delayRun));
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
    
    assertEquals(1, scheduler.tick());
    
    // should be removed for subsequent ticks
    assertEquals(0, scheduler.tick());
    
    assertEquals(1, tr.getRunCount());
  }

  @Test (expected = IllegalArgumentException.class)
  public void tickFail() {
    scheduler.tick(scheduler.getLastTickTime() - 1);
    fail("Exception should have been thrown");
  }
  
  @Test
  public void hasTaskReadyToRunTest() {
    assertFalse(scheduler.hasTaskReadyToRun());
    
    scheduler.schedule(DoNothingRunnable.instance(), 1);
    assertFalse(scheduler.hasTaskReadyToRun());
    
    scheduler.execute(DoNothingRunnable.instance());
    assertTrue(scheduler.hasTaskReadyToRun());
  }
  
  @Test
  public void getDelayTillNextTaskTest() {
    assertEquals(Long.MAX_VALUE, scheduler.getDelayTillNextTask());
    
    scheduler.schedule(DoNothingRunnable.instance(), 1);
    assertEquals(1, scheduler.getDelayTillNextTask());
    
    scheduler.execute(DoNothingRunnable.instance());
    assertEquals(0, scheduler.getDelayTillNextTask());
  }
  
  @Test
  public void clearTasksTest() {
    assertTrue(scheduler.clearTasks().isEmpty());
    
    TestRunnable executeTask = new TestRunnable();
    TestRunnable scheduleTask = new TestRunnable();
    scheduler.execute(executeTask);
    scheduler.schedule(scheduleTask, 1);
    
    List<Runnable> result = scheduler.clearTasks();
    
    assertEquals(2, result.size());
    assertTrue(result.contains(executeTask));
    assertTrue(result.contains(scheduleTask));
  }
}
