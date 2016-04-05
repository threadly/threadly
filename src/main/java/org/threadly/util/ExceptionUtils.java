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
  protected static final short INITIAL_BUFFER_PAD_AMOUNT_PER_TRACE_LINE = 16;
  protected static final short INITIAL_BUFFER_PAD_AMOUNT_FOR_STACK = 64;
  protected static final ThreadLocal<ExceptionHandler> THREAD_LOCAL_EXCEPTION_HANDLER;
  protected static final InheritableThreadLocal<ExceptionHandler> INHERITED_EXCEPTION_HANDLER;
  protected static volatile ExceptionHandler defaultExceptionHandler = null;
  
  static {
    THREAD_LOCAL_EXCEPTION_HANDLER = new ThreadLocal<ExceptionHandler>();
    INHERITED_EXCEPTION_HANDLER = new InheritableThreadLocal<ExceptionHandler>();
  }
  
  /**
   * Sets the {@link ExceptionHandler} for this thread.  This exception handler will be 
   * called if this thread calls to {@link ExceptionUtils#handleException(Throwable)}.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setThreadExceptionHandler(ExceptionHandler exceptionHandler) {
    THREAD_LOCAL_EXCEPTION_HANDLER.set(exceptionHandler);
  }
  
  /**
   * Sets the {@link ExceptionHandler} for this thread, and any threads that spawn off of this 
   * thread.  If this thread, or any children threads (that do not override their 
   * {@link ExceptionHandler}), calls {@link ExceptionUtils#handleException(Throwable)}, 
   * the provided interface will be called.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setInheritableExceptionHandler(ExceptionHandler exceptionHandler) {
    INHERITED_EXCEPTION_HANDLER.set(exceptionHandler);
  }
  
  /**
   * Sets the default {@link ExceptionHandler} to be used by all threads.  Assuming a threads 
   * local, or inheritable {@link ExceptionHandler} has not been set, this default instance will 
   * be relied on.
   * 
   * @param exceptionHandler Exception handler instance, or {@code null} to remove any handler
   */
  public static void setDefaultExceptionHandler(ExceptionHandler exceptionHandler) {
    defaultExceptionHandler = exceptionHandler;
  }

  /**
   * Gets the thread local {@link ExceptionHandler} if one is set, or {@code null} if none 
   * is set.  Since {@link #getExceptionHandler()} prioritizes to the thread local handler, this can 
   * be used to get a reference to the current handler before changing the thread local handler to 
   * ensure that {@link #getExceptionHandler()} (and down stream use like 
   * {@link #handleException(Throwable)}) invoke a handler of your choosing.  Once done you can then 
   * choose to reset the original handler with the one returned from this invocation.
   * 
   * @return Thread local ExceptionHandler, or {@code null} if none is set
   */
  public static ExceptionHandler getThreadLocalExceptionHandler() {
    return THREAD_LOCAL_EXCEPTION_HANDLER.get();
  }
  
  /**
   * Gets the set {@link ExceptionHandler} if one is set, or {@code null} if none is set.  
   * This prioritizes to the threads locally set handler, with the second priority being an inherited 
   * handler, with the final option being the default handler.  If none of those are set, a 
   * {@code null} is returned.
   * 
   * @return Handling instance for this thread, or {@code null} if none are available
   */
  public static ExceptionHandler getExceptionHandler() {
    ExceptionHandler eh = THREAD_LOCAL_EXCEPTION_HANDLER.get();
    if (eh != null) {
      return eh;
    }
    eh = INHERITED_EXCEPTION_HANDLER.get();
    if (eh != null) {
      return eh;
    }
    return defaultExceptionHandler;
  }
  
  /**
   * Invokes {@link Runnable#run()} on the provided runnable on this thread, ensuring that no 
   * throwables are thrown out of this invocation.  If any throwable's are thrown, they will be 
   * provided to {@link #handleException(Throwable)}.
   * 
   * @param r Runnable to invoke, can not be null
   */
  public static void runRunnable(Runnable r) {
    try {
      r.run();
    } catch (Throwable t) {
      handleException(t);
    }
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
      ExceptionHandler ehi = getExceptionHandler();
      if (ehi != null) {
        ehi.handleException(t);
      } else {
        Thread currentThread = Thread.currentThread();
        UncaughtExceptionHandler ueHandler = currentThread.getUncaughtExceptionHandler();
        ueHandler.uncaughtException(currentThread, t);
      }
    } catch (Throwable handlerThrown) {
      try {
        System.err.println("Error handling exception: ");
        t.printStackTrace();
        System.err.println("Error thrown when handling exception: ");
        handlerThrown.printStackTrace();
      } catch (Throwable ignored) {
        // sigh...I give up
      }
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
      
      System.arraycopy(originalstack, 1, 
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
   * Checks to see if the provided error, or any causes in the provided error matching the 
   * provided type.  This can be useful when trying to truncate an exception chain to only the 
   * relevant information.  If the goal is only to determine if it exists or not consider using 
   * {@link #hasCauseOfTypes(Throwable, Iterable)}.  If you are only comparing against one exception 
   * type {@link #getCauseOfType(Throwable, Class)} is a better option (and will return without the 
   * need to cast, type thanks to generics).
   * 
   * @param rootError Throwable to start search from
   * @param types Types of throwable classes looking to match against
   * @return Throwable that matches one of the provided types, or {@code null} if none was found
   */
  public static Throwable getCauseOfTypes(Throwable rootError, 
                                          Iterable<? extends Class<? extends Throwable>> types) {
    ArgumentVerifier.assertNotNull(types, "types");
    
    Throwable t = rootError;
    while (t != null) {
      for (Class<?> c : types) {
        if (c.isInstance(t)) {
          return t;
        }
      }
      t = t.getCause();
    }
    return null;
  }
  
  /**
   * Checks to see if the provided error, or any causes in the provided error match the provided 
   * type.  This can be useful when trying to detect conditions where the actual condition may not 
   * be the head cause, nor the root cause (but buried somewhere in the chain).  If the actual 
   * exception is needed consider using {@link #getCauseOfTypes(Throwable, Iterable)}.  If you are 
   * only comparing against one exception type {@link #hasCauseOfType(Throwable, Class)} is a 
   * better option.
   * 
   * @param rootError Throwable to start search from
   * @param types Types of throwable classes looking to match against
   * @return {@code true} if a match was found, false if no exception cause matches any provided types
   */
  public static boolean hasCauseOfTypes(Throwable rootError, 
                                        Iterable<? extends Class<? extends Throwable>> types) {
    return getCauseOfTypes(rootError, types) != null;
  }
  
  /**
   * Checks to see if the provided error, or any causes in the provided error matching the 
   * provided type.  This can be useful when trying to truncate an exception chain to only the 
   * relevant information.  If the goal is only to determine if it exists or not consider using 
   * {@link #hasCauseOfType(Throwable, Class)}.
   * 
   * @param rootError Throwable to start search from
   * @param type Type of throwable classes looking to match against
   * @param <T> Type of throwable to return (must equal or be super type of generic class provided)
   * @return Throwable that matches one of the provided types, or {@code null} if none was found
   */
  @SuppressWarnings("unchecked")  // verified by generics
  public static <T extends Throwable> T getCauseOfType(Throwable rootError, 
                                                       Class<? extends T> type) {
    ArgumentVerifier.assertNotNull(type, "type");
    Throwable t = rootError;
    while (t != null) {
      if (type.isInstance(t)) {
        return (T)t;
      }
      t = t.getCause();
    }
    return null;
  }
  
  /**
   * Checks to see if the provided error, or any causes in the provided error match the provided 
   * type.  This can be useful when trying to detect conditions where the actual condition may not 
   * be the head cause, nor the root cause (but buried somewhere in the chain).  If the actual 
   * exception is needed consider using {@link #getCauseOfType(Throwable, Class)}.
   * 
   * @param rootError Throwable to start search from
   * @param type Type of throwable classes looking to match against
   * @return {@code true} if a match was found, false if no exception cause matches any provided types
   */
  public static boolean hasCauseOfType(Throwable rootError, Class<? extends Throwable> type) {
    return getCauseOfType(rootError, type) != null;
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
