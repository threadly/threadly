package org.threadly.util;

/**
 * <p>Interface for implementation to handle exceptions which occur.  This 
 * is similar to {@link java.lang.Thread.UncaughtExceptionHandler}, except 
 * that exceptions provided to this interface are handled on the same 
 * thread that threw the exception, and the thread that threw it likely 
 * WONT die.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.4.0
 */
public interface ExceptionHandlerInterface {
  /**
   * An exception was thrown on this thread, and is now being provided to 
   * this handler to handle it (possibly just to simply log it occured).
   * 
   * @param thrown Throwable that was thrown, and caught
   */
  public void handleException(Throwable thrown);
}
