package org.threadly.test.concurrent;

import java.util.concurrent.locks.LockSupport;

import org.threadly.util.Clock;

/**
 * <p>{@link TestCondition} in unit test, designed to check a condition for something that is 
 * happening in a different thread.  Allowing a test to efficiently block till the testable 
 * action has finished.</p>
 * 
 * <p>This tool is used often within threadly's own unit tests.  Please use those as examples 
 * using this class.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public abstract class TestCondition {
  private static final int NANOS_IN_MILLISECOND = 1000000;
  private static final int DEFAULT_POLL_INTERVAL = 10;
  private static final int DEFAULT_TIMEOUT = 1000 * 10;
  private static final int SPIN_THRESHOLD = 10;
  
  /**
   * Getter for the conditions current state.
   * 
   * @return condition state, {@code true} if ready
   */
  public abstract boolean get();

  /**
   * Blocks till condition is true, useful for asynchronism operations, waiting for them to 
   * complete in other threads during unit tests.  
   * 
   * This uses a default timeout of 10 seconds, and a poll interval of 10ms
   */
  public void blockTillTrue() {
    blockTillTrue(DEFAULT_TIMEOUT, DEFAULT_POLL_INTERVAL);
  }

  /**
   * Blocks till condition is true, useful for asynchronism operations, waiting for them to 
   * complete in other threads during unit tests.
   * 
   * This uses the default poll interval of 10ms
   * 
   * @param timeoutInMillis time to wait for value to become true
   */
  public void blockTillTrue(int timeoutInMillis) {
    blockTillTrue(timeoutInMillis, DEFAULT_POLL_INTERVAL);
  }
  
  /**
   * Blocks till condition is true, useful for asynchronism operations, waiting for them to 
   * complete in other threads during unit tests.
   * 
   * @param timeoutInMillis time to wait for value to become true
   * @param pollIntervalInMillis time to sleep between checks
   */
  public void blockTillTrue(int timeoutInMillis, int pollIntervalInMillis) {
    long startTime = Clock.accurateTimeMillis();
    long now = Clock.lastKnownTimeMillis();
    boolean lastResult;
    while (! (lastResult = get()) && 
           (now = Clock.accurateTimeMillis()) - startTime < timeoutInMillis) {
      if (pollIntervalInMillis > SPIN_THRESHOLD) {
        LockSupport.parkNanos(NANOS_IN_MILLISECOND * pollIntervalInMillis);
      }
    }
    
    if (! lastResult) {
      throw new ConditionTimeoutException("Still false after " + 
                                            (now - startTime) + "ms");
    }
  }
  
  /**
   * <p>Thrown if condition is still false after a given timeout.</p>
   * 
   * <p>The reason this exception is public is to allow test implementors to catch this exception 
   * specifically and handle it if necessary.</p>
   * 
   * @author jent - Mike Jensen
   */
  public static class ConditionTimeoutException extends RuntimeException {
    private static final long serialVersionUID = 7445447193772617274L;
    
    /**
     * Constructor for new TimeoutException.
     */
    public ConditionTimeoutException() {
      super();
    }
    
    /**
     * Constructor for new TimeoutException.
     * 
     * @param msg Exception message
     */
    public ConditionTimeoutException(String msg) {
      super(msg);
    }
  }
}