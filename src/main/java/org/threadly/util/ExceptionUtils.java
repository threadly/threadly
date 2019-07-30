package org.threadly.util;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.SchedulerService;

/**
 * Utilities for doing basic operations with exceptions.
 * 
 * @since 1.0.0
 */
public class ExceptionUtils {
  protected static final short INITIAL_BUFFER_PAD_AMOUNT_PER_TRACE_LINE = 16;
  protected static final short INITIAL_BUFFER_PAD_AMOUNT_FOR_STACK = 64;
  protected static final short CAUSE_CYCLE_DEPTH_TRIGGER = 20;
  protected static final ThreadLocal<ExceptionHandler> THREAD_LOCAL_EXCEPTION_HANDLER;
  protected static final InheritableThreadLocal<ExceptionHandler> INHERITED_EXCEPTION_HANDLER;
  protected static volatile ExceptionHandler defaultExceptionHandler = null;
  private static final int DEFAULT_OVERFLOW_CHECK_MILLIS = 10_000;
  private static final SchedulerService EXCEPTION_SCHEDULER = 
      CentralThreadlyPool.isolatedTaskPool(); // low overhead due to rare task submission
  private static final Runnable STACK_OVERFLOW_TASK;
  private static volatile Error handledError = null;
  private static volatile Throwable handledErrorCause = null;
  
  static {
    THREAD_LOCAL_EXCEPTION_HANDLER = new ThreadLocal<>();
    INHERITED_EXCEPTION_HANDLER = new InheritableThreadLocal<>();
    STACK_OVERFLOW_TASK = () -> {
      if (handledError != null) {
        Error toReportError = handledError;
        
        StackSuppressedRuntimeException stackOverflow = 
            new StackSuppressedRuntimeException("Swallowed " + toReportError.getClass().getSimpleName() + 
                                                  " (others may have been missed)", 
                                                handledErrorCause);
        stackOverflow.setStackTrace(toReportError.getStackTrace());
        
        handledErrorCause = null;
        handledError = null;
        
        handleException(stackOverflow);
      }
    };
    changeStackOverflowCheckFrequency(DEFAULT_OVERFLOW_CHECK_MILLIS);
  }
  
  /**
   * Update how often the check for StackOverflowErrors being produced in 
   * {@link #handleException(Throwable)}.  It is important that {@link #handleException(Throwable)} 
   * never throws an exception, however due to the nature of {@link StackOverflowError} there may 
   * be issues in properly reporting the issue.  Instead they will be stored internally with a 
   * best effort reporting to indicate that they have been occurring (but only one instance per 
   * frequency will be reported).  By default they are checked every 10 seconds to see if any were 
   * thrown.  If there was any {@link StackOverflowError} they will be provided back to 
   * {@link #handleException(Throwable)} on an async thread (as to provide a fresh / shortened 
   * stack).
   * 
   * @param frequencyMillis Milliseconds between checks for overflow, or negative to disable checks
   */
  public static synchronized void changeStackOverflowCheckFrequency(int frequencyMillis) {
    EXCEPTION_SCHEDULER.remove(STACK_OVERFLOW_TASK);
    if (frequencyMillis > 0) {
      EXCEPTION_SCHEDULER.scheduleAtFixedRate(STACK_OVERFLOW_TASK, frequencyMillis, frequencyMillis);
    }
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
    } catch (StackOverflowError soe) {
      handledErrorCause = t; // must be set first
      handledError = soe;
    } catch (Throwable handlerThrown) {
      try {
        System.err.println("Error handling exception: ");
        t.printStackTrace();
        System.err.println("Error thrown when handling exception: ");
        handlerThrown.printStackTrace();
      } catch (Error soe) {
        handledErrorCause = t; // must be set first
        handledError = soe;
      } catch (Throwable ignored) {
        // sigh...I give up
      }
    }
  }
  
  /**
   * Remove the top functions from the stack of the provided throwable.  
   * <p>
   * If you invoke this with the quantity of methods to remove being greater than or equal to the 
   * length of the stack of the provided throwable then an IllegalStateException will be thrown.
   * 
   * @param t Throwable to modify
   * @param methodsToRemove Number of top method calls to remove
   */
  protected static void trimStack(Throwable t, int methodsToRemove) {
    StackTraceElement[] originalstack = t.getStackTrace();
    if (originalstack.length <= methodsToRemove) {
      throw new IllegalArgumentException("Can not remove entire stack");
    }
    StackTraceElement[] newStack = new StackTraceElement[originalstack.length - methodsToRemove];
    
    System.arraycopy(originalstack, methodsToRemove, newStack, 0, newStack.length);
    
    t.setStackTrace(newStack);
  }
  
  /**
   * Makes a r{@link RuntimeException} if necessary.  If provided exception is already a 
   * {@link RuntimeException} then it is just directly returned.  If it has to produce a new 
   * exception the stack is updated to omit this call.
   * <p>
   * If the point of wrapping the stack is not useful in debugging consider providing a 
   * {@code true} into. {@link #makeRuntime(Throwable, boolean)}.
   * 
   * @param t {@link Throwable} which may or may not be a runtimeException
   * @return a {@link RuntimeException} based on provided {@link Throwable}
   */
  public static RuntimeException makeRuntime(Throwable t) {
    if (t instanceof RuntimeException) {
      return (RuntimeException)t;
    } else {
      // code duplicated for simpler logic around modifying the stack
      TransformedException result = new TransformedException(t);
      trimStack(result, 1);
      
      return result;
    }
  }

  /**
   * Makes a r{@link RuntimeException} if necessary.  If provided exception is already a 
   * {@link RuntimeException} then it is just directly returned.  If it has to produce a new 
   * exception, you can control if a stack is generated by providing a {@code true} to suppress the 
   * generation (which in java can be fairly expensive).  If stack generation is not suppressed (ie 
   * {@code false} is specified), then the stack will be modified to omit this call.
   * 
   * @param t {@link Throwable} which may or may not be a runtimeException
   * @param suppressWrappedStack {@code true} to avoid generating a stack trace
   * @return A {@link RuntimeException} based on provided {@link Throwable}
   */
  public static RuntimeException makeRuntime(Throwable t, boolean suppressWrappedStack) {
    if (t instanceof RuntimeException) {
      return (RuntimeException)t;
    } else {
      RuntimeException result;
      if (suppressWrappedStack) {
        result = new TransformedSuppressedStackException(t);
      } else {
        // code duplicated for simpler logic around modifying the stack
        result = new TransformedException(t);
        trimStack(result, 1);
      }
      
      return result;
    }
  }
  
  /**
   * Gets the root cause of a provided {@link Throwable}.  If there is no cause for the 
   * {@link Throwable} provided into this function, the original {@link Throwable} is returned.
   * <p>
   * If a cyclic exception chain is detected this function will return the cause where the cycle 
   * end, and thus the returned Throwable will have a cause associated to it (specifically a cause 
   * which is one that was seen earlier in the chain).  If no cycle exists this will return a 
   * Throwable which contains no cause.
   * 
   * @param throwable starting {@link Throwable}
   * @return root cause {@link Throwable}
   */
  public static Throwable getRootCause(Throwable throwable) {
    ArgumentVerifier.assertNotNull(throwable, "throwable");
    
    Throwable result = throwable;
    Throwable cause;
    int depth = 0;
    while ((cause = result.getCause()) != null) {
      result = cause;
      
      if (++depth > CAUSE_CYCLE_DEPTH_TRIGGER) {
        // must restart in case we already passed the source of the start of the cycle
        ArrayList<Throwable> causeChain = unwrapThrowableCycleAwareCauseChain(throwable);
        return causeChain.get(causeChain.size() - 1);
      }
    }
    
    return result;
  }
  
  /**
   * Unwraps the causes of the provided {@link Throwable} into a {@link ArrayList}.  As this list 
   * is built it checks for possible cycles which might exist in the causes.  The returned list 
   * will not contain any cycles.  If the last exception in the list has a cause, it will point 
   * to a {@link Throwable} that is already contained in the list.
   * 
   * @param throwable Throwable to start search from, and first one included in result list
   * @return List of causes from the source {@link Throwable}
   */
  protected static ArrayList<Throwable> unwrapThrowableCycleAwareCauseChain(Throwable throwable) {
    ArrayList<Throwable> causeChain = new ArrayList<>();
    causeChain.add(throwable);
    Throwable cause = throwable;
    while ((cause = cause.getCause()) != null) {
      // slightly more efficient than `contains` and handles any odd overrides of `equals` in the throwable
      // search backwards to optimize cycles towards end of chain
      for (int i = causeChain.size() - 1; i > -1; i--) {
        if (causeChain.get(i) == cause) {
          return causeChain;
        }
      }
      causeChain.add(cause);
    }
    return causeChain;
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
    if (rootError == null) {
      return null;
    }
    
    Throwable t = rootError;
    int depth = 0;
    do {
      for (Class<?> c : types) {
        if (c.isInstance(t)) {
          return t;
        }
      }

      if (++depth > CAUSE_CYCLE_DEPTH_TRIGGER) {
        ArrayList<Throwable> causeChain = unwrapThrowableCycleAwareCauseChain(t);
        for (int i = causeChain.size() - 1; i > 0 /* already checked head of chain */; i--) {
          t = causeChain.get(i);
          for (Class<?> c : types) {
            if (c.isInstance(t)) {
              return t;
            }
          }
        }
        return null;
      }
      
      t = t.getCause();
    } while (t != null && t != rootError);
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
    if (rootError == null) {
      return null;
    }
    
    Throwable t = rootError;
    int depth = 0;
    do {
      if (type.isInstance(t)) {
        return (T)t;
      } else if (++depth > CAUSE_CYCLE_DEPTH_TRIGGER) {
        ArrayList<Throwable> causeChain = unwrapThrowableCycleAwareCauseChain(t);
        for (int i = causeChain.size() - 1; i > 0 /* already checked head of chain */; i--) {
          t = causeChain.get(i);
          if (type.isInstance(t)) {
            return (T)t;
          }
        }
        return null;
      }
      
      t = t.getCause();
    } while (t != null && t != rootError);
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
    if (stack == null) {
      return "";
    }
    
    StringBuilder sb = new StringBuilder(stack.length * INITIAL_BUFFER_PAD_AMOUNT_PER_TRACE_LINE);
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
      stringBuilder.append("\t at ").append(ste.toString()).append(System.lineSeparator());
    }
  }
  
  /**
   * Exception which is constructed from {@link #makeRuntime(Throwable)} when the exception was 
   * not a runtime exception.
   * 
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
    protected TransformedException(Throwable t) {
      super(t == null ? null : t.getMessage(), t);
    }
  }
  
  /**
   * Exception which is constructed from {@link #makeRuntime(Throwable, boolean)} when the 
   * exception was not a runtime exception, and stack is being suppressed.
   * 
   * @since 4.8.0
   */
  public static class TransformedSuppressedStackException extends StackSuppressedRuntimeException {
    private static final long serialVersionUID = 6501962264714125183L;

    /**
     * Constructs a new TransformedSuppressedStackException.
     * 
     * @param message message for exception
     * @param t throwable cause
     */
    protected TransformedSuppressedStackException(Throwable t) {
      super(t == null ? null : t.getMessage(), t);
    }
  }
}
