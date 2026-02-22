package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class StackSuppressedRuntimeExceptionTest extends ThreadlyTester {
  @Test
  public void suppressTest() {
    assertSuppressed(new StackSuppressedRuntimeException());
    assertSuppressed(new StackSuppressedRuntimeException("foo"));
    assertSuppressed(new StackSuppressedRuntimeException(new StackSuppressedRuntimeException()));
    assertSuppressed(new StackSuppressedRuntimeException("foo", new StackSuppressedRuntimeException()));
  }
  
  private static void assertSuppressed(StackSuppressedRuntimeException e) {
    StackTraceElement[] stack = e.getStackTrace();
    for (StackTraceElement ste : stack) {
      assertFalse(ste.getClassName().equals(StackSuppressedRuntimeExceptionTest.class.getName()));
    }
  }
  
  @Test
  public void unsuppressTest() {
    assertTrue(ExceptionUtils.stackToString(new UnsppressedTestException())
                             .contains("StackSuppressedRuntimeExceptionTest"));
  }
  
  @SuppressWarnings("serial")
  private static class UnsppressedTestException extends StackSuppressedRuntimeException {
    @Override
    protected boolean suppressStackGeneration() {
      return false;
    }
  }
}
