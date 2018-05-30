package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public abstract class SubmitterExecutorInterfaceTest extends ThreadlyTester {
  protected abstract SubmitterExecutorFactory getSubmitterExecutorFactory();
  
  @BeforeClass
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  protected static List<TestRunnable> executeTestRunnables(Executor executor, int runnableSleepTime) {
    List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable(runnableSleepTime);
        executor.execute(tr);
        runnables.add(tr);
      }
      
      return runnables;
  }
  
  @Test
  public void executeTest() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(2, false);
      
      List<TestRunnable> runnables = executeTestRunnables(executor, 0);
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
      
      // run one more time now that all workers are already running
      it = runnables.iterator();
      while (it.hasNext()) {
        executor.execute(it.next());
      }
      
      // verify second execution
      it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished(1000, 2);
        
        assertEquals(2, tr.getRunCount());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void executeInOrderTest() throws InterruptedException, TimeoutException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(1, false);
      final AsyncVerifier av = new AsyncVerifier();
      TestRunnable lastRun = null;
      int testQty = 0;
      while (testQty < TEST_QTY) {
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
        
        executor.execute(lastRun);
      }
      
      av.waitForTest(10 * 1000, testQty);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void executeWithFailureRunnableTest() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        if (i % 2 == 0) {
          // add a failure runnable
          executor.execute(new TestRuntimeFailureRunnable());
        }
        TestRunnable tr = new TestRunnable();
        executor.execute(tr);
        runnables.add(tr);
      }
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(1, false);
      
      executor.execute(null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = executor.submit(tr);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
      
      // run one more time now that all workers are already running
      it = runnables.iterator();
      while (it.hasNext()) {
        executor.submit(it.next());
      }
      
      // verify second execution
      it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished(1000, 2);
        
        assertEquals(2, tr.getRunCount());
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
  
  @Test
  public void submitRunnableExceptionTest() throws InterruptedException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      RuntimeException failure = new SuppressedStackRuntimeException();
      TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable(failure);
      ListenableFuture<?> future = executor.submit(tr);
      // no exception should propagate
      
      try {
        future.get();
        fail("Exception should have thrown");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() == failure);
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      List<Future<TestRunnable>> futures = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<TestRunnable> future = executor.submit(tr, tr);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
      
      // run one more time now that all workers are already running
      it = runnables.iterator();
      while (it.hasNext()) {
        executor.submit(it.next());
      }
      
      // verify second execution
      it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished(1000, 2);
        
        assertEquals(2, tr.getRunCount());
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
  
  @Test
  public void submitRunnableWithResultExceptionTest() throws InterruptedException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      RuntimeException failure = new SuppressedStackRuntimeException();
      TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable(failure);
      ListenableFuture<?> future = executor.submit(tr, new Object());
      // no exception should propagate
      
      tr.blockTillFinished();
      assertTrue(future.isDone());
      try {
        future.get();
        fail("Exception should have thrown");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() == failure);
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestCallable> callables = new ArrayList<>(TEST_QTY);
      List<Future<Object>> futures = new ArrayList<>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestCallable tc = new TestCallable();
        Future<Object> future = executor.submit(tc);
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
  
  @Test
  public void submitCallableExceptionTest() throws InterruptedException {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      final RuntimeException failure = new SuppressedStackRuntimeException();
      ListenableFuture<?> future = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          throw failure;
        }
      });
      // no exception should propagate
      
      try {
        future.get();
        fail("Exception should have thrown");
      } catch (ExecutionException e) {
        assertTrue(e.getCause() == failure);
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Runnable)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableWithResultFail() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Runnable)null, new Object());
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SubmitterExecutorFactory factory = getSubmitterExecutorFactory();
    try {
      SubmitterExecutor executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Callable<Void>)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public interface SubmitterExecutorFactory {
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable);

    public void shutdown();
  }
}
