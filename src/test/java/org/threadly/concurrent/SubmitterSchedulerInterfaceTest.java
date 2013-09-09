package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.concurrent.SimpleSchedulerInterfaceTest.SimpleSchedulerFactory;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerInterfaceTest {
  public static void submitRunnableTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, false);
      
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
        Future<?> future = futureIt.next();
        assertTrue(future.isDone());
        assertNull(future.get());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitRunnableWithResultTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<TestRunnable>> futures = new ArrayList<Future<TestRunnable>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestRunnable tr = new TestRunnable();
        Future<TestRunnable> future = scheduler.submit(tr, tr);
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
      
      it = runnables.iterator();
      Iterator<Future<TestRunnable>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> future = futureIt.next();
        assertTrue(future.isDone());
        assertTrue(future.get() == it.next());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitCallableTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, false);
      
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
        
        assertTrue(tc.isDone());
      }
      
      it = callables.iterator();
      Iterator<Future<Object>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<Object> future = futureIt.next();
        TestCallable tc = it.next();
  
        assertTrue(tc.getReturnedResult() == future.get());
        assertTrue(future.isDone());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitRunnableFail(SubmitterSchedulerFactory factory) {
    try {
      SubmitterSchedulerInterface scheduler = factory.make(1, false);
      
      scheduler.submit((Runnable)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitCallableFail(SubmitterSchedulerFactory factory) {
    try {
      SubmitterSchedulerInterface scheduler = factory.make(1, false);
      
      scheduler.submit((Callable<Object>)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitScheduledRunnableTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, true);
      
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
        Future<?> future = futureIt.next();
        assertTrue(future.isDone());
        assertNull(future.get());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitScheduledRunnableWithResultTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<TestRunnable>> futures = new ArrayList<Future<TestRunnable>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestRunnable tr = new TestRunnable();
        Future<TestRunnable> future = scheduler.submitScheduled(tr, tr, scheduleDelay);
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
      
      it = runnables.iterator();
      Iterator<Future<TestRunnable>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> future = futureIt.next();
        assertTrue(future.isDone());
        assertTrue(future.get() == it.next());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitScheduledCallableTest(SubmitterSchedulerFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SubmitterSchedulerInterface scheduler = factory.make(runnableCount, true);
      
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
  
        assertTrue(tc.getReturnedResult() == future.get());
        assertTrue(future.isDone());
        
        long executionDelay = tc.getDelayTillFirstRun();
        assertTrue(executionDelay >= scheduleDelay);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (scheduleDelay + 2000));
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitScheduledRunnableFail(SubmitterSchedulerFactory factory) {
    try {
      SubmitterSchedulerInterface scheduler = factory.make(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitScheduledCallableFail(SubmitterSchedulerFactory factory) {
    try {
      SubmitterSchedulerInterface scheduler = factory.make(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  public interface SubmitterSchedulerFactory extends SimpleSchedulerFactory {
    public SubmitterSchedulerInterface make(int poolSize, boolean prestartIfAvailable);

    public void shutdown();
  }
}
