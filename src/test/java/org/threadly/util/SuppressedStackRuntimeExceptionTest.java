package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class SuppressedStackRuntimeExceptionTest {
  @Test
  public void suppressTest() {
    assertSuppressed(new SuppressedStackRuntimeException());
    assertSuppressed(new SuppressedStackRuntimeException("foo"));
    assertSuppressed(new SuppressedStackRuntimeException(new SuppressedStackRuntimeException()));
    assertSuppressed(new SuppressedStackRuntimeException("foo", new SuppressedStackRuntimeException()));
  }
  
  private static void assertSuppressed(SuppressedStackRuntimeException e) {
    StackTraceElement[] stack = e.getStackTrace();
    for (StackTraceElement ste : stack) {
      assertFalse(ste.getClassName().equals(SuppressedStackRuntimeExceptionTest.class.getName()));
    }
  }
  
  @Test
  public void unsuppressTest() {
    assertTrue(ExceptionUtils.stackToString(new UnsppressedTestException())
                             .contains("SuppressedStackRuntimeExceptionTest"));
  }
  
  @SuppressWarnings("serial")
  private static class UnsppressedTestException extends SuppressedStackRuntimeException {
    @Override
    protected boolean suppressStackGeneration() {
      return false;
    }
  }
}
