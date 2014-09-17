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
  private static final short INITIAL_BUFFER_PAD_AMOUNT_PER_TRACE_LINE = 16;
  private static final short INITIAL_BUFFER_PAD_AMOUNT_FOR_STACK = 64;
  private static final ThreadLocal<ExceptionHandlerInterface> THREAD_LOCAL_EXCEPTION_HANDLER;
  private static final InheritableThreadLocal<ExceptionHandlerInterface> INHERITED_EXCEPTION_HANDLER;
  private static volatile ExceptionHandlerInterface defaultExceptionHandler = null;
  
  static {
    THREAD_LOCAL_EXCEPTION_HANDLER = new ThreadLocal<ExceptionHandlerInterface>();
    INHERITED_EXCEPTION_HANDLER = new InheritableThreadLocal<ExceptionHandlerInterface>();
  }
  
  private ExceptionUtils() {
    // don't construct
  }
  
  /**
   * Sets the {@link ExceptionHandlerInterface} for this thread.  This exception handler will be 
   * called if this thread calls to {@link ExceptionUtils#handleException(Throwable)}.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setThreadExceptionHandler(ExceptionHandlerInterface exceptionHandler) {
    THREAD_LOCAL_EXCEPTION_HANDLER.set(exceptionHandler);
  }
  
  /**
   * Sets the {@link ExceptionHandlerInterface} for this thread, and any threads that spawn off of 
   * this thread.  If this thread, or any children threads (that do not override their 
   * {@link ExceptionHandlerInterface}), calls {@link ExceptionUtils#handleException(Throwable)}, 
   * the provided interface will be called.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setInheritableExceptionHandler(ExceptionHandlerInterface exceptionHandler) {
    INHERITED_EXCEPTION_HANDLER.set(exceptionHandler);
  }
  
  /**
   * Sets the default {@link ExceptionHandlerInterface} to be used by all threads.  Assuming 
   * a threads local, or inheritable {@link ExceptionHandlerInterface} has not been set, this 
   * default instance will be relied on.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setDefaultExceptionHandler(ExceptionHandlerInterface exceptionHandler) {
    defaultExceptionHandler = exceptionHandler;
  }
  
  /**
   * Gets the set {@link ExceptionHandlerInterface} if one is set, or null if none are set.  This 
   * prioritizes to the threads locally set handler, with the second priority being an inherited 
   * handler, with the final option being the default handler.  If none of those are set, a 
   * {@code null} is returned.
   * 
   * @return Handling instance for this thread, or {@code null} if none are available
   */
  public static ExceptionHandlerInterface getExceptionHandler() {
    ExceptionHandlerInterface ehi = THREAD_LOCAL_EXCEPTION_HANDLER.get();
    if (ehi != null) {
      return ehi;
    }
    ehi = INHERITED_EXCEPTION_HANDLER.get();
    if (ehi != null) {
      return ehi;
    }
    return defaultExceptionHandler;
  }
  
  /**
   * This call handles an uncaught throwable.  If a default uncaught exception handler is set, 
   * then that will be called to handle the uncaught exception.  If none is set, then the 
   * exception will be printed out to standard error.
   * 
   * @param t throwable to handle
   */
  public static void handleException(Throwable t) {
    if (t == null) {
      return;
    }
    
    try {
      ExceptionHandlerInterface ehi = getExceptionHandler();
      if (ehi != null) {
        ehi.handleException(t);
      } else {
        Thread currentThread = Thread.currentThread();
        UncaughtExceptionHandler ueHandler = currentThread.getUncaughtExceptionHandler();
        ueHandler.uncaughtException(currentThread, t);
      }
    } catch (Throwable handlerThrown) {
      System.err.println("Error handling exception: ");
      t.printStackTrace();
      System.err.println("Error thrown when handling exception: ");
      handlerThrown.printStackTrace();
    }
  }
  
  /**
   * Makes a runtime exception if necessary.  If provided exception is already runtime then that 
   * is just removed.  If it has to produce a new exception the stack is updated to omit this call.
   * 
   * @param t {@link Throwable} which may or may not be a runtimeException
   * @return a {@link RuntimeException} based on provided exception
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
   * Gets the root cause of a provided {@link Throwable}.  If there is no cause for the 
   * {@link Throwable} provided into this function, the original {@link Throwable} is returned.
   * 
   * @param throwable starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  public static Throwable getRootCause(Throwable throwable) {
    ArgumentVerifier.assertNotNull(throwable, "throwable");
    
    Throwable result = throwable;
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
  @SuppressWarnings("resource")
  public static void writeStackTo(Throwable t, StringBuilder sb) {
    writeStackTo(t, new StringBuilderWriter(sb));
  }
  
  /**
   * Formats and writes a throwable's stack trace to a provided {@link StringBuffer}.
   * 
   * @param t {@link Throwable} which contains stack
   * @param sb StringBuffer to write output to
   */
  @SuppressWarnings("resource")
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
   * Writes the stack trace array out to a string.  This produces a stack trace string in a very 
   * similar way as the {@link #stackToString(Throwable)} from a throwable would.
   * 
   * @param stack Array of stack elements to build the string off of
   * @return String which is the stack in a human readable format
   */
  public static String stackToString(StackTraceElement[] stack) {
    StringBuilder sb = new StringBuilder(stack == null ? 0 : stack.length * INITIAL_BUFFER_PAD_AMOUNT_PER_TRACE_LINE);
    writeStackTo(stack, sb);
    
    return sb.toString();
  }
  
  /**
   * Writes the stack to the provided StringBuilder.  This produces a stack trace string in 
   * a very similar way as the {@link #writeStackTo(Throwable, StringBuilder)} would.
   * 
   * @param stack Array of stack elements to build the string off of
   * @param stringBuilder StringBuilder to write the stack out to
   */
  public static void writeStackTo(StackTraceElement[] stack, StringBuilder stringBuilder) {
    if (stack == null) {
      return;
    }
    ArgumentVerifier.assertNotNull(stringBuilder, "stringBuilder");
    
    for (StackTraceElement ste : stack) {
      stringBuilder.append("\t at ").append(ste.toString()).append(StringUtils.NEW_LINE);
    }
  }
  
  /**
   * <p>Exception which is constructed from {@link #makeRuntime(Throwable)} when the exception was 
   * not a runtime exception.</p>
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
