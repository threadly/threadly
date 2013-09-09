package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.NoThreadScheduler;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTest {
  private static final int TEST_QTY = 10;
  
  private NoThreadScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
  }
  
  @After
  public void tearDown() {
    scheduler = null;
  }
  
  private List<TestRunnable> getRunnableList() {
    List<TestRunnable> result = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      result.add(new TestRunnable());
    }
    
    return result;
  }
  
  private List<TestCallable> getCallableList() {
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
  public void executeTest() {
    List<TestRunnable> runnables = getRunnableList();
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // all should run now
    assertEquals(scheduler.tick(), TEST_QTY);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    // verify no more run after a second tick
    assertEquals(scheduler.tick(), 0);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
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
    assertEquals(scheduler.tick(), TEST_QTY);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    // verify no more run after a second tick
    assertEquals(scheduler.tick(), 0);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
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
    assertEquals(scheduler.tick(), TEST_QTY);
    
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

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);

    assertEquals(executeRun.getRunCount(), 1);   // should have run
    assertEquals(scheduleRun.getRunCount(), 0);  // should NOT have run yet
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 1);
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should have run
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should NOT have run again
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

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);

    assertEquals(submitRun.getRunCount(), 1);   // should have run
    assertEquals(scheduleRun.getRunCount(), 0);  // should NOT have run yet
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 1);
    
    assertEquals(submitRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should have run
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(submitRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should NOT have run again
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

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);

    assertTrue(submitRun.isDone());   // should have run
    assertFalse(scheduleRun.isDone());  // should NOT have run yet
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 1);
    
    assertTrue(scheduleRun.isDone());  // should have run
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
  }
  
  @Test
  public void recurringTest() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);  // should have run
    assertEquals(initialDelay.getRunCount(), 0);  // should NOT have run yet

    assertEquals(scheduler.tick(startTime + delay), 2);
    
    assertEquals(immediateRun.getRunCount(), 2);  // should have run again
    assertEquals(initialDelay.getRunCount(), 1);  // should have run for the first time
    
    assertEquals(scheduler.tick(startTime + (delay * 2)), 2);
    
    assertEquals(immediateRun.getRunCount(), 3);  // should have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should have run again
    
    assertEquals(scheduler.tick(startTime + (delay * 2)), 0); // should not execute anything
    
    assertEquals(immediateRun.getRunCount(), 3);  // should NOT have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should NOT have run again
  }
  
  @Test
  public void removeTest() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);
    
    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);   // should have run
    assertEquals(initialDelay.getRunCount(), 0);  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    assertEquals(scheduler.tick(startTime + delay), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(initialDelay.getRunCount(), 1);  // should have run
    
    assertEquals(scheduler.tick(startTime + delay), 0); // should not execute anything
    
    assertEquals(immediateRun.getRunCount(), 1);   // should NOT have run
    assertEquals(initialDelay.getRunCount(), 1);  // should NOT have run
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tickFail() {
    long now;
    scheduler.tick(now = System.currentTimeMillis());
    
    scheduler.tick(now - 1);
    fail("Exception should have been thrown");
  }
}
