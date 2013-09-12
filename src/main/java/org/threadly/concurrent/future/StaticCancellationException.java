package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;

/**
 * Class to hold a singleton CancellationException to prevent needing to construct it.
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
