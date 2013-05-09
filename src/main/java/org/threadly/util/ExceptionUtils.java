package org.threadly.util;

/**
 * Utilities for doing basic things with exceptions.
 * 
 * @author jent - Mike Jensen
 */
public class ExceptionUtils {
  /**
   * Makes a runtime exception if necessary.  If provided exception
   * is already runtime then that is just removed.  If it has to produce
   * a new exception the stack is updated to ommit this call.
   * 
   * @param t Throwable which may or may not be a runtimeException
   * @return a runtime exception based on provided exception
   */
  public static RuntimeException makeRuntime(Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException)t;
    } else {
      RuntimeException result = new RuntimeException(t);
      
      // remove this function from the stack trace
      StackTraceElement[] originalstack = result.getStackTrace();
      StackTraceElement[] newStack = new StackTraceElement[originalstack.length - 1];
      
      System.arraycopy(originalstack, 0, 
                       newStack, 0, newStack.length);
      
      result.setStackTrace(newStack);
      
      return result;
    }
  }
}
