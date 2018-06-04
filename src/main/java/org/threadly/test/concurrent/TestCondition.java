package org.threadly.test.concurrent;

import java.util.concurrent.locks.LockSupport;
import java.util.function.IntBinaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * {@link TestCondition} in unit test, designed to check a condition for something that is 
 * happening in a different thread.  Allowing a test to efficiently block till the testable 
 * action has finished.
 * <p>
 * This tool is used often within threadly's own unit tests.  Please use those as examples using 
 * this class.
 * 
 * @since 1.0.0
 */
public class TestCondition {
  protected static final int DEFAULT_POLL_INTERVAL = 10;
  protected static final int DEFAULT_TIMEOUT = 10_000;
  
  private static void delay(long maxMillis) {
    if (maxMillis > 10) {
      LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * maxMillis);
    } else {
      Thread.yield();
    }
  }
  
  private final IntBinaryOperator blockCondition;
  
  /**
   * This constructor is expected to have {@link #get()} overridden, otherwise an exception will be 
   * thrown when this condition is checked/used.
   */
  public TestCondition() {
    this(null);
  }

  /**
   * Construct a new {@link TestCondition} with a provided supplier for when the state changes.  
   * Alternatively this can be constructed with {@link #TestCondition()} and then the condition can 
   * be reported by overriding {@link #get()}.
   * 
   * @since 5.0
   * @param condition Condition to check for
   */
  public TestCondition(Supplier<Boolean> condition) {
    blockCondition = (timeoutInMillis, pollIntervalInMillis) -> {
      long startTime = Clock.accurateForwardProgressingMillis();
      long now = startTime;
      boolean lastResult;
      while (! (lastResult = condition == null ? get() : condition.get()) && 
             ! Thread.currentThread().isInterrupted() && 
             (now = Clock.accurateForwardProgressingMillis()) - startTime < timeoutInMillis) {
        delay(pollIntervalInMillis);
      }
      
      if (lastResult) {
        return 0; // ignored result
      } else {
        throw new ConditionTimeoutException("Still 'false' after " + 
                                              (now - startTime) + "ms, interrupted: " + 
                                              Thread.currentThread().isInterrupted());
      }
    };
  }
  
  /**
   * Construct a new {@link TestCondition} where one function will supply a result and a second 
   * function will test that result to see if the condition is met.
   * 
   * @param <T> the type of object returned from the supplier and tested by the predicate
   * @param supplier The function to provide a result to test
   * @param predicate The predicate a result is provided to for checking the test condition
   */
  public <T> TestCondition(Supplier<? extends T> supplier, Predicate<? super T> predicate) {
    ArgumentVerifier.assertNotNull(supplier, "supplier");
    ArgumentVerifier.assertNotNull(predicate, "predicate");
    
    blockCondition = (timeoutInMillis, pollIntervalInMillis) -> {
      long startTime = Clock.accurateForwardProgressingMillis();
      long now = startTime;
      boolean pass;
      T lastResult;
      while (! (pass = predicate.test(lastResult = supplier.get())) && 
             ! Thread.currentThread().isInterrupted() && 
             (now = Clock.accurateForwardProgressingMillis()) - startTime < timeoutInMillis) {
        delay(pollIntervalInMillis);
      }
      
      if (pass) {
        return 0; // ignored result
      } else {
        throw new ConditionTimeoutException("Still '" + lastResult + "' after " + 
                                              (now - startTime) + "ms, interrupted: " + 
                                              Thread.currentThread().isInterrupted());
      }
    };
  }
  
  /**
   * Getter for the conditions current state.  If constructed with {@link #TestCondition()} this 
   * must be overridden.  Otherwise this will return the result from the provided supplier at 
   * construction.
   * 
   * @return Test condition state, {@code true} if ready
   */
  public boolean get() {
    throw new RuntimeException("Must override get() or provide condition supplier");
  }

  /**
   * Blocks till condition is true, useful for asynchronism operations, waiting for them to 
   * complete in other threads during unit tests.  
   * <p>
   * This uses a default timeout of 10 seconds, and a poll interval of 10ms
   */
  public void blockTillTrue() {
    blockTillTrue(DEFAULT_TIMEOUT, DEFAULT_POLL_INTERVAL);
  }

  /**
   * Blocks till condition is true, useful for asynchronism operations, waiting for them to 
   * complete in other threads during unit tests.
   * <p>
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
    blockCondition.applyAsInt(timeoutInMillis, pollIntervalInMillis);
  }
  
  /**
   * Thrown if condition is still false after a given timeout.
   * <p>
   * The reason this exception is public is to allow test implementors to catch this exception 
   * specifically and handle it if necessary.
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