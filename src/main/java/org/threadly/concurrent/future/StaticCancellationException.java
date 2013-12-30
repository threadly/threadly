package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;

/**
 * <p>Class to hold a singleton CancellationException to prevent needing to construct it.</p>
 * 
 * <p>This is not designed to be used by anyone outside of threadly.  If a callable throws this 
 * exception it will be interpreted as a cancellation rather than an execution exception.</p>
 * 
 * @author jent - Mike Jensen
 */
public class StaticCancellationException {
  private static final CancellationException CANCELLATION_EXCEPTION;
  
  static {
    CANCELLATION_EXCEPTION = new CancellationException();
    CANCELLATION_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }
  
  /**
   * Call to get the singleton instance.
   * 
   * @return the single instance of CancellationException
   */
  public static CancellationException instance() {
    return CANCELLATION_EXCEPTION;
  }
}
