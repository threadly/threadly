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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTest {
  private NoThreadScheduler blockingScheduler;
  private NoThreadScheduler nonblockingScheduler;
  
  @Before
  public void setup() {
    blockingScheduler = new NoThreadScheduler(true);
    nonblockingScheduler = new NoThreadScheduler(false);
  }
  
  @After
  public void tearDown() {
    blockingScheduler = null;
    nonblockingScheduler = null;
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
    assertFalse(blockingScheduler.isShutdown());
    assertFalse(nonblockingScheduler.isShutdown());
  }
  
  @Test
  public void executeTest() throws InterruptedException {
    executeTest(blockingScheduler);
    executeTest(nonblockingScheduler);
  }
  
  private void executeTest(NoThreadScheduler scheduler) throws InterruptedException {
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
    
    if (scheduler == nonblockingScheduler) {
      // verify no more run after a second tick
      assertEquals(scheduler.tick(), 0);
      
      it = runnables.iterator();
      while (it.hasNext()) {
        assertEquals(1, it.next().getRunCount());
      }
    }
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException {
    submitRunnableTest(blockingScheduler);
    submitRunnableTest(nonblockingScheduler);
  }
  
  private void submitRunnableTest(NoThreadScheduler scheduler) throws InterruptedException {
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
    
    if (scheduler == nonblockingScheduler) {
      // verify no more run after a second tick
      assertEquals(0, scheduler.tick());
      
      it = runnables.iterator();
      while (it.hasNext()) {
        assertEquals(1, it.next().getRunCount());
      }
    }
    
    Iterator<Future<?>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      assertTrue(futureIt.next().isDone());
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, 
                                          ExecutionException {
    submitCallableTest(blockingScheduler);
    submitCallableTest(nonblockingScheduler);
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
  public void scheduleRunnableTest() throws InterruptedException {
    scheduleRunnableTest(blockingScheduler);
    scheduleRunnableTest(nonblockingScheduler);
  }
  
  private static void scheduleRunnableTest(NoThreadScheduler scheduler) throws InterruptedException {
    TestRunnable tr = new TestRunnable();
    long scheduleTime = System.currentTimeMillis();
    scheduler.schedule(tr, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick();
    }
    long runTime = System.currentTimeMillis();
    
    assertEquals(1, runCount);
    
    assertTrue(tr.ranOnce());
    assertTrue((runTime - scheduleTime) >= DELAY_TIME);
  }
  
  @Test
  public void submitScheduledRunnableTest() throws InterruptedException {
    submitScheduledRunnableTest(blockingScheduler);
    submitScheduledRunnableTest(nonblockingScheduler);
  }
  
  private static void submitScheduledRunnableTest(NoThreadScheduler scheduler) throws InterruptedException {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = scheduler.submitScheduled(tr, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick();
    }
    
    assertEquals(1, runCount);
    
    assertTrue(tr.getDelayTillFirstRun() >= DELAY_TIME);
    assertTrue(future.isDone());
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    submitScheduledRunnableFail(blockingScheduler);
    submitScheduledRunnableFail(nonblockingScheduler);
  }
  
  private static void submitScheduledRunnableFail(NoThreadScheduler scheduler) {
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
    submitScheduledCallableTest(blockingScheduler);
    submitScheduledCallableTest(nonblockingScheduler);
  }
  
  private static void submitScheduledCallableTest(NoThreadScheduler scheduler) throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable();
    ListenableFuture<?> future = scheduler.submitScheduled(tc, DELAY_TIME);
    
    int runCount = 0;
    while (runCount == 0) {
      runCount = scheduler.tick();
    }
    
    assertEquals(1, runCount);
    
    assertTrue(tc.getDelayTillFirstRun() >= DELAY_TIME);
    assertTrue(future.isDone());
    assertTrue(future.get() == tc.getReturnedResult());
  }
  
  @Test
  public void scheduleWithFixedZeroDelayTest() throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    final TestRunnable[] testRunnables = new TestRunnable[TEST_QTY];
    for (int i = 0; i < TEST_QTY; i++) {
      testRunnables[i] = new TestRunnable();
    }
    
    nonblockingScheduler.scheduleWithFixedDelay(new Runnable() {
      private int runIndex = -1;
      
      @Override
      public void run() {
        if (runIndex >= 0) {
          // verify they ran ahead of us
          av.assertTrue(testRunnables[runIndex].ranOnce());
        }
        
        if (++runIndex < testRunnables.length) {
          nonblockingScheduler.execute(testRunnables[runIndex]);
        } else {  // we are done
          // remove task so .tick can unblock
          nonblockingScheduler.remove(this);
          av.signalComplete();
        }
      }
    }, 0, 0);
    
    nonblockingScheduler.tick();
    
    av.waitForTest();
  }
  
  @Test
  public void submitScheduledCallableFail() {
    submitScheduledCallableFail(blockingScheduler);
    submitScheduledCallableFail(nonblockingScheduler);
  }
  
  private static void submitScheduledCallableFail(NoThreadScheduler scheduler) {
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
  public void scheduleWithFixedDelayFail() {
    scheduleWithFixedDelayFail(blockingScheduler);
    scheduleWithFixedDelayFail(nonblockingScheduler);
  }
  
  private static void scheduleWithFixedDelayFail(NoThreadScheduler scheduler) {
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
  public void removeRunnableTest() {
    removeRunnableTest(blockingScheduler);
    removeRunnableTest(nonblockingScheduler);
  }
  
  private static void removeRunnableTest(NoThreadScheduler scheduler) {
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
    
    assertFalse(blockingScheduler.remove(immediateRun));
    
    blockingScheduler.scheduleWithFixedDelay(immediateRun, 0, DELAY_TIME);
    assertTrue(blockingScheduler.remove(immediateRun));
    
    blockingScheduler.scheduleWithFixedDelay(immediateRun, 0, DELAY_TIME);
    blockingScheduler.scheduleWithFixedDelay(initialDelay, DELAY_TIME, DELAY_TIME);
    
    assertEquals(1, blockingScheduler.tick());
    
    assertEquals(1, immediateRun.getRunCount());   // should have run
    assertEquals(0, initialDelay.getRunCount());  // should NOT have run yet
    
    assertTrue(blockingScheduler.remove(immediateRun));
    
    assertEquals(1, blockingScheduler.tick());
    
    assertEquals(1, immediateRun.getRunCount());   // should NOT have run again
    assertEquals(1, initialDelay.getRunCount());  // should have run
  }
  
  @Test
  public void removeCallableTest() throws InterruptedException {
    TestCallable immediateRun = new TestCallable();
    TestCallable delayRun = new TestCallable();
    
    assertFalse(blockingScheduler.remove(immediateRun));
    
    blockingScheduler.submitScheduled(immediateRun, 0);
    assertTrue(blockingScheduler.remove(immediateRun));
    assertFalse(blockingScheduler.remove(immediateRun));
    
    blockingScheduler.submitScheduled(delayRun, DELAY_TIME);
    
    assertEquals(1, blockingScheduler.tick());
    
    assertFalse(immediateRun.isDone());
    assertTrue(delayRun.isDone());
  }
  
  @Test
  public void removeWhileRunningTest() throws InterruptedException {
    removeWhileRunningTest(blockingScheduler);
    removeWhileRunningTest(nonblockingScheduler);
  }
  
  private void removeWhileRunningTest(final NoThreadScheduler scheduler) throws InterruptedException {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() {
        assertTrue(scheduler.remove(this));
      }
    };
    
    scheduler.scheduleWithFixedDelay(tr, 0, 0);
    
    assertEquals(1, scheduler.tick());
    
    if (scheduler == nonblockingScheduler) {
      // should be removed for subsequent ticks
      assertEquals(0, scheduler.tick());
      
      assertEquals(1, tr.getRunCount());
    }
  }
  
  @Test
  public void blockTillAvailableExecuteTest() throws InterruptedException, TimeoutException {
    final AsyncVerifier av = new AsyncVerifier();
    TestRunnable tickRunnable = new TestRunnable() {
      @Override
      public void handleRunStart() {
        try {
          int runCount = blockingScheduler.tick();  // should block
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
    blockingScheduler.execute(testTask);
    
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
          long startTime = System.currentTimeMillis();
          blockingScheduler.schedule(testTask, DELAY_TIME);
          int runCount = blockingScheduler.tick();  // should block
          long finishTime = System.currentTimeMillis();
          
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
  public void hasTaskReadyToRunTest() throws InterruptedException {
    assertFalse(blockingScheduler.hasTaskReadyToRun());
    assertFalse(nonblockingScheduler.hasTaskReadyToRun());
    
    // schedule in the future
    blockingScheduler.schedule(new TestRunnable(), 1000 * 15);
    nonblockingScheduler.schedule(new TestRunnable(), 1000 * 15);
    
    // still should have nothing ready to run
    assertFalse(blockingScheduler.hasTaskReadyToRun());
    assertFalse(nonblockingScheduler.hasTaskReadyToRun());
    
    blockingScheduler.execute(new TestRunnable());
    nonblockingScheduler.execute(new TestRunnable());
    
    // should now have tasks ready to run
    assertTrue(blockingScheduler.hasTaskReadyToRun());
    assertTrue(nonblockingScheduler.hasTaskReadyToRun());
    
    blockingScheduler.tick();
    nonblockingScheduler.tick();
    
    // should no longer have anything to run
    assertFalse(blockingScheduler.hasTaskReadyToRun());
    assertFalse(nonblockingScheduler.hasTaskReadyToRun());
  }
  
  @Test
  public void hasTaskReadyToRunRunningTaskTest() throws InterruptedException {
    nonblockingScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        assertFalse(nonblockingScheduler.hasTaskReadyToRun());
        
        nonblockingScheduler.execute(new TestRunnable());
        assertTrue(nonblockingScheduler.hasTaskReadyToRun());
        
        nonblockingScheduler.remove(this);
      }
    }, 0, 1000);
    
    nonblockingScheduler.tick();
  }
  
  @Test
  public void clearTasksTest() {
    blockingScheduler.schedule(new TestRunnable(), 1000 * 15);
    nonblockingScheduler.schedule(new TestRunnable(), 1000 * 15);
    blockingScheduler.execute(new TestRunnable());
    nonblockingScheduler.execute(new TestRunnable());
    
    blockingScheduler.clearTasks();
    nonblockingScheduler.clearTasks();
    
    assertEquals(0, blockingScheduler.taskQueue.size());
    assertEquals(0, nonblockingScheduler.taskQueue.size());
  }
}
