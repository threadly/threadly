package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTest {
  private NoThreadScheduler threadSafeScheduler;
  private NoThreadScheduler notSafeScheduler;
  
  @Before
  public void setup() {
    threadSafeScheduler = new NoThreadScheduler(true);
    notSafeScheduler = new NoThreadScheduler(false);
  }
  
  @After
  public void tearDown() {
    threadSafeScheduler = null;
    notSafeScheduler = null;
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
    assertFalse(threadSafeScheduler.isShutdown());
    assertFalse(notSafeScheduler.isShutdown());
  }
  
  @Test
  public void threadSafeExecuteTest() {
    executeTest(threadSafeScheduler);
  }
  
  @Test
  public void executeTest() {
    executeTest(notSafeScheduler);
  }
  
  private static void executeTest(NoThreadScheduler scheduler) {
    List<TestRunnable> runnables = getRunnableList();
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick());
    
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
  public void threadSafeSubmitRunnableTest() {
    submitRunnableTest(threadSafeScheduler);
  }
  
  @Test
  public void submitRunnableTest() {
    submitRunnableTest(notSafeScheduler);
  }
  
  private static void submitRunnableTest(NoThreadScheduler scheduler) {
    List<TestRunnable> runnables = getRunnableList();
    List<Future<?>> futures = new ArrayList<Future<?>>(runnables.size());
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      Future<?> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick());
    
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
  public void threadSafeSubmitCallableTest() throws InterruptedException, 
                                                    ExecutionException {
    submitCallableTest(threadSafeScheduler);
  }

  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    submitCallableTest(notSafeScheduler);
  }
  
  private static void submitCallableTest(NoThreadScheduler scheduler) throws InterruptedException, 
                                                                             ExecutionException {
    List<TestCallable> callables = getCallableList();
    List<Future<Object>> futures = new ArrayList<Future<Object>>(callables.size());
    Iterator<TestCallable> it = callables.iterator();
    while (it.hasNext()) {
      Future<Object> future = scheduler.submit(it.next());
      assertNotNull(future);
      futures.add(future);
    }
    
    // all should run now
    assertEquals(TEST_QTY, scheduler.tick());
    
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
  public void threadSafeScheduleRunnableTest() {
    scheduleRunnableTest(threadSafeScheduler);
  }
  
  @Test
  public void scheduleRunnableTest() {
    scheduleRunnableTest(notSafeScheduler);
  }
  
  private static void scheduleRunnableTest(NoThreadScheduler scheduler) {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable executeRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    scheduler.schedule(scheduleRun, scheduleDelay);
    scheduler.execute(executeRun);

    long startTime = System.currentTimeMillis();
    assertEquals(1, scheduler.tick(startTime));

    assertEquals(1, executeRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, scheduler.tick(startTime + scheduleDelay));
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(1, executeRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void threadSafeSubmitScheduledRunnableTest() {
    submitScheduledRunnableTest(threadSafeScheduler);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    submitScheduledRunnableTest(notSafeScheduler);
  }
  
  private static void submitScheduledRunnableTest(NoThreadScheduler scheduler) {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable submitRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    Future<?> future = scheduler.submit(submitRun);
    assertNotNull(future);
    future = scheduler.submitScheduled(scheduleRun, scheduleDelay);
    assertNotNull(future);

    long startTime = System.currentTimeMillis();
    assertEquals(1, scheduler.tick(startTime));

    assertEquals(1, submitRun.getRunCount());   // should have run
    assertEquals(0, scheduleRun.getRunCount());  // should NOT have run yet
    
    assertEquals(1, scheduler.tick(startTime + scheduleDelay));
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should have run
    
    assertEquals(0, scheduler.tick(startTime + scheduleDelay)); // should not execute anything
    
    assertEquals(1, submitRun.getRunCount());   // should NOT have run again
    assertEquals(1, scheduleRun.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void threadSafeSubmitScheduledCallableTest() {
    submitScheduledCallableTest(threadSafeScheduler);
  }
  
  @Test
  public void submitScheduledCallableTest() {
    submitScheduledCallableTest(notSafeScheduler);
  }
  
  private static void submitScheduledCallableTest(NoThreadScheduler scheduler) {
    long scheduleDelay = 1000 * 10;
    
    TestCallable submitRun = new TestCallable();
    TestCallable scheduleRun = new TestCallable();
    
    Future<?> future = scheduler.submit(submitRun);
    assertNotNull(future);
    future = scheduler.submitScheduled(scheduleRun, scheduleDelay);
    assertNotNull(future);

    long startTime = System.currentTimeMillis();
    assertEquals(1, scheduler.tick(startTime));

    assertTrue(submitRun.isDone());   // should have run
    assertFalse(scheduleRun.isDone());  // should NOT have run yet
    
    assertEquals(1, scheduler.tick(startTime + scheduleDelay));
    
    assertTrue(scheduleRun.isDone());  // should have run
    
    assertEquals(0, scheduler.tick(startTime + scheduleDelay)); // should not execute anything
  }
  
  @Test
  public void threadSafeRecurringTest() {
    recurringTest(threadSafeScheduler);
  }
  
  @Test
  public void recurringTest() {
    recurringTest(notSafeScheduler);
  }
  
  private static void recurringTest(NoThreadScheduler scheduler) {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    assertEquals(1, scheduler.tick(startTime));
    
    assertEquals(1, immediateRun.getRunCount());  // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet

    assertEquals(2, scheduler.tick(startTime + delay));
    
    assertEquals(2, immediateRun.getRunCount());  // should have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run for the first time
    
    assertEquals(2, scheduler.tick(startTime + (delay * 2)));
    
    assertEquals(3, immediateRun.getRunCount());  // should have run again
    assertEquals(2, initialDelay.getRunCount());  // should have run again
    
    assertEquals(0, scheduler.tick(startTime + (delay * 2))); // should not execute anything
    
    assertEquals(3, immediateRun.getRunCount());  // should NOT have run again
    assertEquals(2, initialDelay.getRunCount());  // should NOT have run again
  }
  
  @Test
  public void threadSafeRemoveTest() {
    removeTest(threadSafeScheduler);
  }
  
  @Test
  public void removeTest() {
    removeTest(notSafeScheduler);
  }
  
  private static void removeTest(NoThreadScheduler scheduler) {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);
    
    long startTime = System.currentTimeMillis();
    assertEquals(1, scheduler.tick(startTime));
    
    assertEquals(1, immediateRun.getRunCount());   // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    assertEquals(1, scheduler.tick(startTime + delay));
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run
    
    assertEquals(0, scheduler.tick(startTime + delay)); // should not execute anything
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run
    assertEquals(1, initialDelay.getRunCount());  // should NOT have run
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void threadSafeTickFail() {
    tickFail(threadSafeScheduler);
  }

  @Test (expected = IllegalArgumentException.class)
  public void tickFail() {
    tickFail(notSafeScheduler);
  }
  
  private static void tickFail(NoThreadScheduler scheduler) {
    long now;
    scheduler.tick(now = System.currentTimeMillis());
    
    scheduler.tick(now - 1);
    fail("Exception should have been thrown");
  }
}
