package org.threadly.concurrent.wrapper;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings("javadoc")
public class ThrowableSuppressingRunnableTest {
  @Test
  public void getContainedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    ThrowableSuppressingRunnable tsr = new ThrowableSuppressingRunnable(tr);
    
    assertTrue(tsr.getContainedRunnable() == tr);
  }
  
  @Test
  public void nullRunTest() {
    Runnable tsr = new ThrowableSuppressingRunnable(null);
    
    tsr.run();
    // no exception should throw
  }
  
  @Test
  public void runTest() {
    TestRunnable tr = new TestRunnable();
    Runnable tsr = new ThrowableSuppressingRunnable(tr);
    
    tsr.run();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void runExceptionTest() {
    TestExceptionHandler teh = new TestExceptionHandler();
    final RuntimeException testException = new RuntimeException();
    ExceptionUtils.setThreadExceptionHandler(teh);
    TestRunnable exceptionRunnable = new TestRuntimeFailureRunnable(testException);
    Runnable tsr = new ThrowableSuppressingRunnable(exceptionRunnable);
    
    tsr.run();

    assertEquals(1, teh.getCallCount());
    assertEquals(Thread.currentThread(), teh.getCalledWithThread());
    assertEquals(testException, teh.getLastThrowable());
  }
}
