package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.future.ListenableFuture;

@SuppressWarnings("javadoc")
public class SameThreadSubmitterExecutorTest extends SubmitterExecutorInterfaceTest {
  private SameThreadSubmitterExecutor executor;
  
  @BeforeClass
  public static void classSetup() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
  }
  
  @Before
  public void setup() {
    executor = new SameThreadSubmitterExecutor();
  }
  
  @After
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
