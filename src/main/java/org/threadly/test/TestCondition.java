package org.threadly.test;

/**
 * TestCondition in unit test, designed to check a condition
 * for something that is happening in a different thread.
 * 
 * @author jent - Mike Jensen
 */
public abstract class TestCondition {
  private static final int DEFAULT_POLL_INTERVAL = 20;
  private static final int DEFAULT_TIMEOUT = 1000 * 10;
  
  /**
   * @return condition state
   */
  public abstract boolean get();

  /**
   * Blocks till condition is true, useful for asynchronism operations, 
   * waiting for them to complete in other threads during unit tests.
   * 
   * This uses a default timeout of 10 seconds, and a poll interval of 20ms
   */
  public void blockTillTrue() {
    blockTillTrue(DEFAULT_TIMEOUT, DEFAULT_POLL_INTERVAL);
  }

  /**
   * Blocks till condition is true, useful for asynchronism operations, 
   * waiting for them to complete in other threads during unit tests.
   * 
   * This uses the default poll interval of 20ms
   * 
   * @param timeout time to wait for value to become true
   */
  public void blockTillTrue(int timeout) {
    blockTillTrue(timeout, DEFAULT_POLL_INTERVAL);
  }
  
  /**
   * Blocks till condition is true, useful for asynchronism operations, 
   * waiting for them to complete in other threads during unit tests.
   * 
   * @param timeout time to wait for value to become true
   * @param pollInterval time to sleep between checks
   */
  public void blockTillTrue(int timeout, int pollInterval) {
    long startTime = System.currentTimeMillis();
    while (! get() && 
           System.currentTimeMillis() - startTime < timeout) {
      if (pollInterval > 10) { // might as well spin if < 10
        try {
          Thread.sleep(pollInterval);
        } catch (InterruptedException e) {
          // ignored
        }
      }
    }
    
    if (! get()) {
      throw new TimeoutException("Still false after " + 
                                   (System.currentTimeMillis() - startTime) + "ms");
    }
  }
  
  /**
   * Thrown if condition is still false after a given timeout
   * 
   * @author jent - Mike Jensen
   */
  public static class TimeoutException extends RuntimeException {
    private static final long serialVersionUID = 7445447193772617274L;
    
    /**
     * Constructor for new TimeoutException
     */
    public TimeoutException() {
      super();
    }
    
    /**
     * Constructor for new TimeoutException
     * 
     * @param msg Exception message
     */
    public TimeoutException(String msg) {
      super(msg);
    }
  }
}