package org.threadly.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SubmitterExecutorInterfaceTest {
  public static void executeTest(SubmitterExecutorFactory factory) {
    try {
      int runnableCount = 10;
      
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
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
      int runnableCount = 10;
      
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
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
  
  public static void submitRunnableWithResultTest(SubmitterExecutorFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<TestRunnable>> futures = new ArrayList<Future<TestRunnable>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
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
  
  public static void submitCallableTest(SubmitterExecutorFactory factory) throws InterruptedException, ExecutionException {
    try {
      int runnableCount = 10;
      
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(runnableCount, false);
      
      List<TestCallable> callables = new ArrayList<TestCallable>(runnableCount);
      List<Future<Object>> futures = new ArrayList<Future<Object>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestCallable tc = new TestCallable(0);
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
  
  public static void submitRunnableFail(SubmitterExecutorFactory factory) {
    try {
      SubmitterExecutorInterface executor = factory.makeSubmitterExecutor(1, false);
      
      executor.submit((Runnable)null);
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
