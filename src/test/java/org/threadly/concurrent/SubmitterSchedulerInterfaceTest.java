package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public abstract class SubmitterSchedulerInterfaceTest extends SubmitterExecutorInterfaceTest {
  protected abstract SubmitterSchedulerFactory getSubmitterSchedulerFactory();

  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return getSubmitterSchedulerFactory();
  }
  
  @Override
  @Test
  public void executeInOrderTest() throws InterruptedException, TimeoutException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface executor = factory.makeSubmitterScheduler(1, false);
      final AsyncVerifier av = new AsyncVerifier();
      TestRunnable lastRun = null;
      long startTime = System.currentTimeMillis();
      int testQty = 0;
      while (testQty < TEST_QTY || System.currentTimeMillis() - startTime < 100) {
        testQty++;
        final TestRunnable fLastRun = lastRun;
        lastRun = new TestRunnable() {
          @Override
          public void handleRunStart() {
            if (fLastRun != null) {
              av.assertTrue(fLastRun.ranOnce());
            }
            av.signalComplete();
          }
        };
        executor.schedule(new TestRunnable(), 5);
        executor.execute(lastRun);
      }
      
      av.waitForTest(10 * 1000, testQty);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void scheduleTest() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SimpleSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        scheduler.schedule(tr, DELAY_TIME);
        runnables.add(tr);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= DELAY_TIME);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (DELAY_TIME + 2000));
        assertEquals(1, tr.getRunCount());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void scheduleNoDelayTest() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SimpleSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      TestRunnable tr = new TestRunnable();
      scheduler.schedule(tr, 0);
      tr.blockTillStarted();
      assertEquals(1, tr.getRunCount());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableNoDelayTest() throws InterruptedException, ExecutionException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      TestRunnable tr = new TestRunnable();
      ListenableFuture<?> f = scheduler.submitScheduled(tr, 0);
      assertNotNull(f);
      tr.blockTillFinished();
      assertEquals(1, tr.getRunCount());
      assertNull(f.get());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableNoDelayTest() throws InterruptedException, ExecutionException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      TestCallable tc = new TestCallable(0);
      ListenableFuture<?> f = scheduler.submitScheduled(tc, 0);
      assertNotNull(f);
      assertTrue(tc.getReturnedResult() == f.get());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void scheduleFail() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SimpleSchedulerInterface scheduler = factory.makeSubmitterScheduler(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    recurringExecutionTest(false, true);
  }
  
  @Test
  public void scheduleWithFixedDelayInitialDelayTest() {
    recurringExecutionTest(true, true);
  }
  
  @Test
  public void scheduleAtFixedRateTest() {
    recurringExecutionTest(false, false);
  }
  
  @Test
  public void scheduleAtFixedRateInitialDelayTest() {
    recurringExecutionTest(true, false);
  }
  
  private void recurringExecutionTest(boolean initialDelay, boolean fixedDelay) {
    final long initialDelayInMillis = initialDelay ? DELAY_TIME : 0;
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SimpleSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
  
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable(fixedDelay ? 0 : DELAY_TIME);
        if (fixedDelay) {
          scheduler.scheduleWithFixedDelay(tr, initialDelayInMillis, DELAY_TIME);
        } else {
          scheduler.scheduleAtFixedRate(tr, initialDelayInMillis, DELAY_TIME);
        }
        runnables.add(tr);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished((TEST_QTY * (DELAY_TIME * (CYCLE_COUNT + 1))) + 2000, CYCLE_COUNT);
        if (initialDelay) {
          long executionDelay = tr.getDelayTillFirstRun();
          assertTrue(executionDelay >= DELAY_TIME);
          // should be very timely with a core pool size that matches runnable count
          assertTrue(executionDelay <= (DELAY_TIME + 2000));
        }
        
        tr.blockTillFinished((DELAY_TIME * (CYCLE_COUNT - 1)) + 2000, CYCLE_COUNT);
        long executionDelay = tr.getDelayTillRun(CYCLE_COUNT);
        assertTrue(executionDelay >= DELAY_TIME * (CYCLE_COUNT - 1));
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (DELAY_TIME * (CYCLE_COUNT - 1)) + 2000);
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SimpleSchedulerInterface scheduler = factory.makeSubmitterScheduler(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<Future<?>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submitScheduled(tr, DELAY_TIME);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= DELAY_TIME);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (DELAY_TIME + 2000));  
        assertEquals(1, tr.getRunCount());
      }
      
      Iterator<Future<?>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> future = futureIt.next();
        // future should basically be done already, but we set a bit of a timeout in case of slow systems
        assertNull(future.get(10, TimeUnit.SECONDS));
        assertTrue(future.isDone());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<TestRunnable>> futures = new ArrayList<Future<TestRunnable>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<TestRunnable> future = scheduler.submitScheduled(tr, tr, DELAY_TIME);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= DELAY_TIME);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (DELAY_TIME + 2000));  
        assertEquals(1, tr.getRunCount());
      }
      
      it = runnables.iterator();
      Iterator<Future<TestRunnable>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> future = futureIt.next();
        assertTrue(future.get(10, TimeUnit.SECONDS) == it.next());
        assertTrue(future.isDone());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException, TimeoutException {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(TEST_QTY, true);
      
      List<TestCallable> callables = new ArrayList<TestCallable>(TEST_QTY);
      List<Future<Object>> futures = new ArrayList<Future<Object>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestCallable tc = new TestCallable(0);
        Future<Object> future = scheduler.submitScheduled(tc, DELAY_TIME);
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
  
        assertTrue(tc.getReturnedResult() == future.get(10, TimeUnit.SECONDS));
        assertTrue(future.isDone());
        
        long executionDelay = tc.getDelayTillFirstRun();
        assertTrue(executionDelay >= DELAY_TIME);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (DELAY_TIME + 2000));
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(1, false);
      try {
        scheduler.submitScheduled((Runnable)null, 1000);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
      try {
        scheduler.submitScheduled((Runnable)null, null, 1000);
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
      try {
        scheduler.submitScheduled(new TestRunnable(), null, -1);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SubmitterSchedulerFactory factory = getSubmitterSchedulerFactory();
    try {
      SubmitterSchedulerInterface scheduler = factory.makeSubmitterScheduler(1, false);
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
  
  public interface SubmitterSchedulerFactory extends SubmitterExecutorFactory {
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable);
  }
}
