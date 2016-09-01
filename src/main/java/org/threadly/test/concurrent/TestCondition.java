package org.threadly.test.concurrent;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

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
public class TestCondition {
  protected static final int DEFAULT_POLL_INTERVAL = 10;
  protected static final int DEFAULT_TIMEOUT = 1000 * 10;
  protected static final int SPIN_THRESHOLD = 10;
  
  private final Supplier<Boolean> condition;
  
  /**
   * This constructor is expected to have {@link #get()} overridden, otherwise an exception will be 
   * thrown when this condition is checked/used.
   */
  public TestCondition() {
    this(() -> { 
      throw new RuntimeException("Must override get() or provide condition supplier");
    });
  }

  /**
   * Construct a new test condition with a provided supplier for when the state changes.  
   * Alternatively this can be constructed with {@link #TestCondition()} and then the condition can 
   * be reported by overriding {@link #get()}.
   * 
   * @param condition Condition to check
   * @since 5.0.0
   */
  public TestCondition(Supplier<Boolean> condition) {
    this.condition = condition;
  }
  
  /**
   * Getter for the conditions current state.  If constructed with {@link #TestCondition()} this 
   * must be overridden.  Otherwise this will return the result from the provided supplier at 
   * construction.
   * 
   * @return Test condition state, {@code true} if ready
   */
  public boolean get() {
    return condition.get();
  }

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
    long startTime = Clock.accurateForwardProgressingMillis();
    long now = startTime;
    boolean lastResult;
    while (! (lastResult = get()) && ! Thread.currentThread().isInterrupted() && 
           (now = Clock.accurateForwardProgressingMillis()) - startTime < timeoutInMillis) {
      if (pollIntervalInMillis > SPIN_THRESHOLD) {
        LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * pollIntervalInMillis);
      }
    }
    
    if (! lastResult) {
      throw new ConditionTimeoutException("Still false after " + 
                                            (now - startTime) + "ms, interrupted: " + 
                                            Thread.currentThread().isInterrupted());
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