package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class ExceptionUtilsTest {
  @Test
  public void testMakeRuntimeWithRuntime() {
    RuntimeException testException = new RuntimeException();

    RuntimeException resultException = ExceptionUtils.makeRuntime(testException);

    // we expect the exact same reference to come out
    assertTrue(testException == resultException);
  }

  @Test
  public void testMakeRuntimeWithNonRuntime() {
    Exception testException = new Exception();

    RuntimeException resultException = ExceptionUtils.makeRuntime(testException);

    // verify stack trace does not contain Util.makeRuntime inside it for when it created a new exception
    StackTraceElement[] stack = resultException.getStackTrace();
    for (StackTraceElement ste : stack) {
      assertFalse(ste.getClass().getName().equals(ExceptionUtils.class.getName()));
    }

    // verify the cause was our original exception
    assertTrue(testException == resultException.getCause());
  }

}
