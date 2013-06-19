package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class SimpleSchedulerInterfaceTest {
  public static void executeTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, false);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      runnables.add(tr);
    }
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished();
      
      assertEquals(tr.getRunCount(), 1);
    }
    
    // run one more time now that all workers are already running
    it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // verify second execution
    it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished(1000, 2);
      
      assertEquals(tr.getRunCount(), 2);
    }
  }
  
  public static void submitRunnableTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, false);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      Future<?> future = scheduler.submit(tr);
      assertNotNull(future);
      runnables.add(tr);
      futures.add(future);
    }
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished();
      
      assertEquals(tr.getRunCount(), 1);
    }
    
    // run one more time now that all workers are already running
    it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.submit(it.next());
    }
    
    // verify second execution
    it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished(1000, 2);
      
      assertEquals(tr.getRunCount(), 2);
    }
    
    Iterator<Future<?>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      assertTrue(futureIt.next().isDone());
    }
  }
  
  public static void submitCallableTest(PrioritySchedulerFactory factory) throws InterruptedException, ExecutionException {
    int runnableCount = 10;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, false);
    
    List<TestCallable> callables = new ArrayList<TestCallable>(runnableCount);
    List<Future<Object>> futures = new ArrayList<Future<Object>>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestCallable tc = new TestCallable(0);
      Future<Object> future = scheduler.submit(tc);
      assertNotNull(future);
      callables.add(tc);
      futures.add(future);
    }
    
    // verify execution
    Iterator<TestCallable> it = callables.iterator();
    while (it.hasNext()) {
      TestCallable tc = it.next();
      tc.blockTillTrue();
      
      assertTrue(tc.done);
    }
    
    it = callables.iterator();
    Iterator<Future<Object>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      Future<Object> future = futureIt.next();
      TestCallable tc = it.next();

      assertTrue(tc.result == future.get());
      assertTrue(future.isDone());
    }
  }
  
  public static void executeFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    
    scheduler.execute(null);
    fail("Execption should have thrown");
  }
  
  public static void submitRunnableFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    
    scheduler.submit((Runnable)null);
    fail("Execption should have thrown");
  }
  
  public static void submitCallableFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    
    scheduler.submit((Callable<Object>)null);
    fail("Execption should have thrown");
  }
  
  public static void scheduleTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    int scheduleDelay = 50;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, true);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.schedule(tr, scheduleDelay);
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      long executionDelay = tr.getDelayTillFirstRun();
      assertTrue(executionDelay >= scheduleDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (scheduleDelay + 2000));  
      assertEquals(tr.getRunCount(), 1);
    }
  }
  
  public static void submitScheduledRunnableTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    int scheduleDelay = 50;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, true);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      Future<?> future = scheduler.submitScheduled(tr, scheduleDelay);
      assertNotNull(future);
      runnables.add(tr);
      futures.add(future);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      long executionDelay = tr.getDelayTillFirstRun();
      assertTrue(executionDelay >= scheduleDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (scheduleDelay + 2000));  
      assertEquals(tr.getRunCount(), 1);
    }
    
    Iterator<Future<?>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      assertTrue(futureIt.next().isDone());
    }
  }
  
  public static void submitScheduledCallableTest(PrioritySchedulerFactory factory) throws InterruptedException, ExecutionException {
    int runnableCount = 10;
    int scheduleDelay = 50;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, true);
    
    List<TestCallable> callables = new ArrayList<TestCallable>(runnableCount);
    List<Future<Object>> futures = new ArrayList<Future<Object>>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestCallable tc = new TestCallable(0);
      Future<Object> future = scheduler.submitScheduled(tc, scheduleDelay);
      assertNotNull(future);
      callables.add(tc);
      futures.add(future);
    }
    
    // verify execution and execution times
    Iterator<TestCallable> it = callables.iterator();
    Iterator<Future<Object>> futureIt = futures.iterator();
    while (futureIt.hasNext()) {
      Future<Object> future = futureIt.next();
      TestCallable tc = it.next();

      assertTrue(tc.result == future.get());
      assertTrue(future.isDone());
      
      long executionDelay = tc.getDelayTillFirstRun();
      assertTrue(executionDelay >= scheduleDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (scheduleDelay + 2000));
    }
  }
  
  public static void scheduleExecutionFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    try {
      scheduler.schedule(null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.schedule(new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public static void submitScheduledRunnableFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    try {
      scheduler.submitScheduled((Runnable)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.submitScheduled(new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public static void submitScheduledCallableFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    try {
      scheduler.submitScheduled((Callable<Object>)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.submitScheduled(new TestCallable(0), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public static void recurringExecutionTest(PrioritySchedulerFactory factory) {
    final int runnableCount = 10;
    final int recurringDelay = 50;
    final int waitCount = 2;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount, true);
    
    // schedule a task first in case there are any initial startup actions which may be slow
    scheduler.scheduleWithFixedDelay(new TestRunnable(), 0, 1000 * 10);

    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, recurringDelay);
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished((runnableCount * (recurringDelay * waitCount)) + 2000, waitCount);
      long executionDelay = tr.getDelayTillRun(waitCount);
      assertTrue(executionDelay >= recurringDelay * (waitCount - 1));
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (recurringDelay * (waitCount - 1)) + 2000);
    }
  }
  
  public static void recurringExecutionFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1, false);
    try {
      scheduler.scheduleWithFixedDelay(null, 1000, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), -1, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 1000, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public interface PrioritySchedulerFactory {
    public SimpleSchedulerInterface make(int poolSize, boolean prestartIfAvailable);

    public void shutdown();
  }
  
  protected static class TestCallable extends TestCondition 
                                      implements Callable<Object> {
    private final long runDurration;
    private final long creationTime;
    private final Object result;
    private volatile long callTime;
    private volatile boolean done;
    
    public TestCallable(long runDurration) {
      this.runDurration = runDurration;
      this.creationTime = System.currentTimeMillis();
      callTime = -1;
      result = new Object();
      done = false;
    }

    public long getDelayTillFirstRun() {
      return callTime - creationTime;
    }

    @Override
    public Object call() {
      callTime = System.currentTimeMillis();
      TestUtils.sleep(runDurration);
      
      done = true;
      
      return result;
    }

    @Override
    public boolean get() {
      return done;
    }
  }
}
