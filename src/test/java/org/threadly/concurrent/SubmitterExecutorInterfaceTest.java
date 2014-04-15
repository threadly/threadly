package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SubmitterExecutorInterfaceTest {
  public static void executeTest(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
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
  
  public static void executeWithFailureRunnableTest(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
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
  
  public static void executeFail(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(1, false);
      
      executor.execute(null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitRunnableTest(SubmitterExecutorFactory factory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<Future<?>>(TEST_QTY);
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
  
  public static void submitRunnableExceptionTest(SubmitterExecutorFactory factory) throws InterruptedException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      RuntimeException failure = new RuntimeException();
      TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable(failure);
      ListenableFuture<?> future = executor.submit(tr);
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
  
  public static void submitRunnableWithResultTest(SubmitterExecutorFactory factory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<TestRunnable>> futures = new ArrayList<Future<TestRunnable>>(TEST_QTY);
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
  
  public static void submitRunnableWithResultExceptionTest(SubmitterExecutorFactory factory) throws InterruptedException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      RuntimeException failure = new RuntimeException();
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
  
  public static void submitCallableTest(SubmitterExecutorFactory factory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestCallable> callables = new ArrayList<TestCallable>(TEST_QTY);
      List<Future<Object>> futures = new ArrayList<Future<Object>>(TEST_QTY);
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
  
  public static void submitCallableExceptionTest(SubmitterExecutorFactory factory) throws InterruptedException {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(TEST_QTY, false);
      
      final RuntimeException failure = new RuntimeException();
      ListenableFuture<?> future = executor.submit(new Callable<Object>() {
        @Override
        public Object call() throws Exception {
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
  
  public static void submitRunnableFail(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Runnable)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }

  public static void submitRunnableWithResultFail(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Runnable)null, new Object());
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public static void submitCallableFail(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Callable<Object>)null);
      fail("Execption should have thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  public interface SubmitterExecutorFactory {
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable);

    public void shutdown();
  }
}
