package org.threadly.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threadly.concurrent.future.ListenableFuture;

@SuppressWarnings("javadoc")
public class SameThreadSubmitterExecutorTest extends SubmitterExecutorInterfaceTest {
  private SameThreadSubmitterExecutor executor;
  
  @BeforeAll
  public static void classSetup() {
    setIgnoreExceptionHandler();
  }
  
  @BeforeEach
  @SuppressWarnings("deprecation")
  public void setup() {
    executor = new SameThreadSubmitterExecutor();
  }
  
  @AfterEach
  public void cleanup() {
    executor = null;
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorFactory();
  }
  
  @Test
  @Override
  public void executeTest() {
    TestRunnable tr = new TestRunnable();
    executor.execute(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    
    super.executeTest();
  }
  
  @Test
  @Override
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = executor.submit(tr);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == null);

    super.submitRunnableTest();
  }
  
  @Test
  @Override
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    TestRunnable tr = new TestRunnable();
    ListenableFuture<?> future = executor.submit(tr, result);
    
    assertTrue(tr.ranOnce());
    assertTrue(tr.executedThread == Thread.currentThread());
    assertTrue(future.isDone());
    assertTrue(future.get() == result);
    
    super.submitRunnableWithResultTest();
  }
  
  @Test
  public void staticInstanceTest() {
    assertNotNull(SameThreadSubmitterExecutor.instance());
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
    @SuppressWarnings("deprecation")
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return new SameThreadSubmitterExecutor();
    }

    @Override
    public void shutdown() {
      // ignored
    }
  }
}
