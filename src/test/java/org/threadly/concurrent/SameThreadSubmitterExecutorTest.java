package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
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
  
  @Test
  public void executeTest() {
    TestRunnable tr = new TestRunnable();
    executor.execute(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    
    SubmitterExecutorInterfaceTest.executeTest(new ExecutorFactory());
  }
  
  @Test
  public void executeExceptionTest() {
    SubmitterExecutorInterfaceTest.executeWithFailureRunnableTest(new ExecutorFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    SubmitterExecutorInterfaceTest.executeFail(new ExecutorFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SubmitterExecutorInterfaceTest.submitRunnableFail(new ExecutorFactory());
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = executor.submit(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == null);

    SubmitterExecutorInterfaceTest.submitRunnableTest(new ExecutorFactory());
  }
  
  @Test
  public void submitRunnableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableExceptionTest(new ExecutorFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableWithResultFail() {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultFail(new ExecutorFactory());
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
    
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(new ExecutorFactory());
  }
  
  @Test
  public void submitRunnableWithResultExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultExceptionTest(new ExecutorFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SubmitterExecutorInterfaceTest.submitCallableFail(new ExecutorFactory());
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SubmitterExecutorInterfaceTest.submitCallableTest(new ExecutorFactory());
  }
  
  @Test
  public void submitCallableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitCallableExceptionTest(new ExecutorFactory());
  }
  
  private static class TestRunnable extends org.threadly.test.concurrent.TestRunnable {
    private Thread executedThread = null;
    
    @Override
    public void handleRunStart() {
      executedThread = Thread.currentThread();
    }
  }
  
  private static class ExecutorFactory implements SubmitterExecutorFactory {
    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return new SameThreadSubmitterExecutor();
    }

    @Override
    public void shutdown() {
      // ignored
    }
  }
}
