package org.threadly.util;

/**
 * <p>Interface for implementation to handle exceptions which occur.  This is similar to 
 * {@link java.lang.Thread.UncaughtExceptionHandler}, except that exceptions provided to this 
 * interface are handled on the same thread that threw the exception, and the thread that threw it 
 * likely WONT die.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0 (since 2.4.0 as ExceptionHandlerInterface)
 */
public interface ExceptionHandler {
  /**
   * Default {@link ExceptionHandler} implementation which will invoke 
   * {@link Throwable#printStackTrace()}.
   */
  public static final ExceptionHandler PRINT_STACKTRACE_HANDLER = new ExceptionHandler() {
    @Override
    public void handleException(Throwable thrown) {
      thrown.printStackTrace();
    }
  };
  /**
   * Default {@link ExceptionHandler} implementation which will swallow the exception with no action.
   */
  public static final ExceptionHandler IGNORE_HANDLER = new ExceptionHandler() {
    @Override
    public void handleException(Throwable thrown) {
      // ignored
    }
  };
  
  /**
   * An exception was thrown on this thread, and is now being provided to this handler to handle 
   * it (possibly just to simply log it occurred).
   * 
   * @param thrown Throwable that was thrown, and caught
   */
  public void handleException(Throwable thrown);
}
