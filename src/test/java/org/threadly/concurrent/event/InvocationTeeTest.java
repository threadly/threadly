package org.threadly.concurrent.event;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public class InvocationTeeTest extends ThreadlyTester {
  @BeforeAll
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  @Test
  public void teeCreateFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      InvocationTee.tee(null);
      });
  }
  
  @Test
  public void invokeTest() {
    TestRunnable tr = new TestRunnable();
    Runnable r = InvocationTee.tee(Runnable.class, null, tr);
    r.run();
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeTwiceTest() {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    Runnable r = InvocationTee.tee(Runnable.class, tr1, null, tr2, null);
    r.run();
    r.run();
    
    assertEquals(2, tr1.getRunCount());
    assertEquals(2, tr2.getRunCount());
  }
  
  @Test
  public void invokeListenerExceptionTest() {
    TestRunnable tr = new TestRuntimeFailureRunnable();
    Runnable r = InvocationTee.tee(Runnable.class, null, tr);
    r.run();
    // no exception should be thrown
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void teeWithExceptionThrowingCreateFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      InvocationTee.teeWithExceptionThrowing(null);
      });
  }
  
  @Test
  public void invokeWithExceptionThrowingTest() {
    TestRunnable tr = new TestRunnable();
    Runnable r = InvocationTee.teeWithExceptionThrowing(Runnable.class, null, tr);
    r.run();
    // should run just like the normal version
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokeWithExceptionThrowingListenerExceptionTest() {
    RuntimeException thrown = new StackSuppressedRuntimeException();
    TestRunnable tr = new TestRuntimeFailureRunnable(thrown);
    Runnable r = InvocationTee.teeWithExceptionThrowing(Runnable.class, null, tr);
    try {
      r.run();
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      assertTrue(thrown == e);
    }
  }
  
  @Test
  public void teeWithExecutorCreateFail() {
    try {
      InvocationTee.teeWithExecutor(null, Runnable.class);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      InvocationTee.teeWithExecutor(SameThreadSubmitterExecutor.instance(), null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void invokeWithExecutorTest() {
    TestRunnable tr = new TestRunnable();
    Runnable r = InvocationTee.teeWithExecutor(SameThreadSubmitterExecutor.instance(), 
                                               Runnable.class, null, tr);
    r.run();
    // should run just like the normal version
    assertTrue(tr.ranOnce());
  }
}
