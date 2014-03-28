package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.future.ListenableFuture;

@SuppressWarnings("javadoc")
public class SameThreadSubmitterExecutorTest {
  private SameThreadSubmitterExecutor executor;
  
  @BeforeClass
  public static void classSetup() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Before
  public void setup() {
    executor = new SameThreadSubmitterExecutor();
  }
  
  public void tearDown() {
    executor = null;
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    executor.execute(null);
  }
  
  @Test
  public void executeTest() {
    TestRunnable tr = new TestRunnable();
    executor.execute(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
  }
  
  @Test
  public void executeExceptionTest() {
    TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable();
    executor.execute(tr);
    // no exception should propagate
    
    assertTrue(tr.ranOnce());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    executor.submit((Runnable)null);
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = executor.submit(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == null);
  }
  
  @Test
  public void submitRunnableExceptionTest() throws InterruptedException {
    RuntimeException failure = new RuntimeException();
    TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable(failure);
    ListenableFuture<?> future = executor.submit(tr);
    // no exception should propagate
    
    assertTrue(tr.ranOnce());
    assertTrue(future.isDone());
    try {
      future.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableWithResultFail() {
    executor.submit((Runnable)null, new Object());
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = executor.submit(tr, result);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == result);
  }
  
  @Test
  public void submitRunnableWithResultExceptionTest() throws InterruptedException {
    RuntimeException failure = new RuntimeException();
    TestRuntimeFailureRunnable tr = new TestRuntimeFailureRunnable(failure);
    ListenableFuture<?> future = executor.submit(tr, new Object());
    // no exception should propagate
    
    assertTrue(tr.ranOnce());
    assertTrue(future.isDone());
    try {
      future.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    executor.submit((Callable<Object>)null);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable();
    ListenableFuture<?> future = executor.submit(tc);
    
    assertTrue(future.isDone());
    assertTrue(future.get() == tc.getReturnedResult());
  }
  
  @Test
  public void submitCallableExceptionTest() throws InterruptedException {
    final RuntimeException failure = new RuntimeException();
    ListenableFuture<?> future = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        throw failure;
      }
    });
    // no exception should propagate
    
    assertTrue(future.isDone());
    try {
      future.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  private static class TestRunnable extends org.threadly.test.concurrent.TestRunnable {
    private Thread executedThread = null;
    
    @Override
    public void handleRunStart() {
      executedThread = Thread.currentThread();
    }
  }
}
