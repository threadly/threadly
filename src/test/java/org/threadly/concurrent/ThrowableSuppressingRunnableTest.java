package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

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
    UncaughtExceptionHandler originalUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
    TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
    final RuntimeException testException = new RuntimeException();
    Thread.setDefaultUncaughtExceptionHandler(ueh);
    try {
      TestRunnable exceptionRunnable = new TestRuntimeFailureRunnable(testException);
      Runnable tsr = new ThrowableSuppressingRunnable(exceptionRunnable);
      
      tsr.run();
      
      assertEquals(Thread.currentThread(), ueh.getCalledWithThread());
      assertEquals(testException, ueh.getCalledWithThrowable());
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(originalUncaughtExceptionHandler);
    }
  }
}
