package org.threadly.util;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.Thread.UncaughtExceptionHandler;

/**
 * <p>Utilities for doing basic operations with exceptions.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ExceptionUtils {
  private static final short INITIAL_BUFFER_PAD_AMOUNT_FOR_STACK = 32;
  
  private ExceptionUtils() {
    // don't construct
  }
  
  /**
   * This call handles an uncaught throwable.  If a default uncaught exception 
   * handler is set, then that will be called to handle the uncaught exception.  
   * If none is set, then the exception will be printed out to std err.
   * 
   * @param t throwable to handle
   */
  public static void handleException(Throwable t) {
    if (t == null) {
      return;
    }
    
    UncaughtExceptionHandler ueHandler = Thread.getDefaultUncaughtExceptionHandler();
    if (ueHandler != null) {
       ueHandler.uncaughtException(Thread.currentThread(), t);
    } else {
      t.printStackTrace(System.err);
    }
  }
  
  /**
   * Makes a runtime exception if necessary.  If provided exception
   * is already runtime then that is just removed.  If it has to produce
   * a new exception the stack is updated to omit this call.
   * 
   * @param t {@link Throwable} which may or may not be a runtimeException
   * @return a runtime exception based on provided exception
   */
  public static RuntimeException makeRuntime(Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException)t;
    } else {
      TransformedException result = new TransformedException(t == null ? null : t.getMessage(), t);
      
      // remove this function from the stack trace
      StackTraceElement[] originalstack = result.getStackTrace();
      StackTraceElement[] newStack = new StackTraceElement[originalstack.length - 1];
      
      System.arraycopy(originalstack, 0, 
                       newStack, 0, newStack.length);
      
      result.setStackTrace(newStack);
      
      return result;
    }
  }
  
  /**
   * Gets the root cause of a provided {@link Throwable}.  If there is no 
   * cause for the {@link Throwable} provided into this function, the original 
   * {@link Throwable} is returned.
   * 
   * @param t starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  public static Throwable getRootCause(Throwable t) {
    if (t == null) {
      throw new IllegalArgumentException("Must provide input throwable");
    }
    
    Throwable result = t;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    
    return result;
  }
  
  /**
   * Convert throwable's stack and message into a simple string.
   * 
   * @param t throwable which contains stack
   * @return string which contains the throwable stack trace
   */
  public static String stackToString(Throwable t) {
    if (t == null) {
      return "";
    }
    
    String msg = t.getMessage();
    int msgLength = (msg == null ? 0 : msg.length());
    StringBuilder sb = new StringBuilder(msgLength + INITIAL_BUFFER_PAD_AMOUNT_FOR_STACK);
    
    writeStackTo(t, sb);
    
    return sb.toString();
  }
  
  /**
   * Formats and writes a throwable's stack trace to a provided {@link StringBuilder}.
   * 
   * @param t {@link Throwable} which contains stack
   * @param sb StringBuilder to write output to
   */
  public static void writeStackTo(Throwable t, StringBuilder sb) {
    writeStackTo(t, new StringBuilderWriter(sb));
  }
  
  /**
   * Formats and writes a throwable's stack trace to a provided {@link StringBuffer}.
   * 
   * @param t {@link Throwable} which contains stack
   * @param sb StringBuffer to write output to
   */
  public static void writeStackTo(Throwable t, StringBuffer sb) {
    writeStackTo(t, new StringBufferWriter(sb));
  }
  
  /**
   * Formats and writes a throwable's stack trace to a provided {@link Writer}.
   * 
   * @param t {@link Throwable} which contains stack
   * @param w Writer to write output to
   */
  public static void writeStackTo(Throwable t, Writer w) {
    if (t == null) {
      return;
    }
    
    PrintWriter pw = new PrintWriter(w);
    try {
      t.printStackTrace(pw);
      pw.flush();
    } finally {
      pw.close();
    }
  }
  
  /**
   * Writes the stack trace array out to a string.  This produces a stack trace string in 
   * a very similar way as the stackToString from a throwable would.
   * 
   * @param stack Array of stack elements to build the string off of
   * @return String which is the stack in a human readable format
   */
  public static String stackToString(StackTraceElement[] stack) {
    StringBuilder sb = new StringBuilder();
    writeStackTo(stack, sb);
    
    return sb.toString();
  }
  
  /**
   * Writes the stack to the provided StringBuilder.  This produces a stack trace string in 
   * a very similar way as the writeStackTo from a throwable would.
   * 
   * @param stack Array of stack elements to build the string off of
   * @param sb StringBuilder to write the stack out to
   */
  public static void writeStackTo(StackTraceElement[] stack, StringBuilder sb) {
    if (stack == null) {
      return;
    } if (sb == null) {
      throw new IllegalArgumentException("Must provide string builder to write to");
    }
    
    for (StackTraceElement ste : stack) {
      sb.append("\t at ").append(ste.getClassName()).append('.').append(ste.getMethodName());
      if (ste.isNativeMethod()) {
        sb.append("(Native Method)");
      } else {
        sb.append('(').append(ste.getFileName()).append(':').append(ste.getLineNumber()).append(')');
      }
      sb.append("\n");
    }
  }
  
  /**
   * Exception which is constructed from makeRuntime when the exception 
   * was not a runtime exception.
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  public static class TransformedException extends RuntimeException {
    private static final long serialVersionUID = 4524467217814731188L;

    /**
     * Constructs a new TransformedException.
     * 
     * @param message message for exception
     * @param t throwable cause
     */
    protected TransformedException(String message, Throwable t) {
      super(message, t);
    }
  }
}
