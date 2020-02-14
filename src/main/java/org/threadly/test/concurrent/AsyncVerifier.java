package org.threadly.test.concurrent;

import java.util.concurrent.TimeoutException;

import org.threadly.util.Clock;

/**
 * A simple class for verifying multi-threaded unit tests.  If any thread has a failed a failed 
 * {@code assert} or call to {@link #fail()} the main threads call to {@link #waitForTest()} will 
 * throw an exception to indicate the test as a failure.
 * <p>
 * This class also provides a way to control the flow of a unit test by blocking main test thread 
 * until {@link #signalComplete()} is called from the other thread.
 * 
 * @deprecated Now provided by org.threadly:threadly-test:0,1 artifact
 * 
 * @since 1.0.0
 */
@Deprecated
public class AsyncVerifier {
  protected static final int DEFAULT_TIMEOUT = 10_000;
  
  protected final Object notifyLock;
  private int signalCount;
  private RuntimeException failure;
  
  /**
   * Constructs a new {@link AsyncVerifier}.
   */
  public AsyncVerifier() {
    notifyLock = new Object();
    signalCount = 0;
    failure = null;
  }
  
  /**
   * Waits for a default of 10 seconds, or until signalComplete has been called once, or until a 
   * failure occurs.  If signalComplete has been called before this, this call will never block.
   * 
   * @throws InterruptedException Thrown if thread is interrupted while waiting
   * @throws TimeoutException Thrown if timeout occurs without signalComplete being called
   */
  public void waitForTest() throws InterruptedException, TimeoutException {
    waitForTest(DEFAULT_TIMEOUT, 1);
  }
  
  /**
   * Waits a specified amount of time, or until signalComplete has been called once, or until a 
   * failure occurs.  If signalComplete has been called before this, this call will never block.
   * 
   * @param timeoutInMs Timeout to wait for signalComplete action to occur
   * @throws InterruptedException Thrown if thread is interrupted while waiting
   * @throws TimeoutException Thrown if timeout occurs without signalComplete being called
   */
  public void waitForTest(long timeoutInMs) throws InterruptedException, TimeoutException {
    waitForTest(timeoutInMs, 1);
  }
  
  /**
   * Waits a specified amount of time, or until signalComplete has been called a specified amount 
   * of times, or until a failure occurs.
   * <p>
   * If {@link #waitForTest()} is being called multiple times on the same instance, the 
   * signalComplete count is NOT reset.  So you must either create new instances, or pass in a 
   * larger value for the expected signalComplete count.
   * 
   * @param timeoutInMs Timeout to wait for signalComplete action to occur
   * @param signalCount Amount of signalComplete calls to expect before unblocking
   * @throws InterruptedException Thrown if thread is interrupted while waiting
   * @throws TimeoutException Thrown if timeout occurs without signalComplete being called
   */
  public void waitForTest(long timeoutInMs, int signalCount) throws InterruptedException, 
                                                                    TimeoutException {
    long startTime = Clock.accurateForwardProgressingMillis();
    long remainingWaitTime = timeoutInMs;
    synchronized (notifyLock) {
      while (this.signalCount < signalCount && 
             remainingWaitTime > 0 && 
             failure == null) {
        notifyLock.wait(remainingWaitTime);
        
        remainingWaitTime = timeoutInMs - (Clock.accurateForwardProgressingMillis() - startTime);
      }
    }
    
    if (failure != null) {
      throw failure;
    } else if (remainingWaitTime <= 0) {
      throw new TimeoutException();
    }
    // if neither are true we exited normally
  }
  
  /**
   * Call to indicate that this thread has finished, and should notify the waiting main test 
   * thread that the test may be complete.
   */
  public void signalComplete() {
    synchronized (notifyLock) {
      signalCount++;
      
      notifyLock.notifyAll();
    }
  }
  
  /**
   * Verifies that the passed in condition is true.  If it is not an exception is thrown in this 
   * thread, as well as possibly any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param condition condition to verify is {@code true}
   */
  public void assertTrue(boolean condition) {
    if (! condition) {
      synchronized (notifyLock) {
        failure = new TestFailure("assertTrue failure");
        
        notifyLock.notifyAll();
      }
      
      throw failure;
    }
  }
  
  /**
   * Verifies that the passed in condition is false.  If it is not an exception is thrown in this 
   * thread, as well as possibly any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param condition condition to verify is {@code false}
   */
  public void assertFalse(boolean condition) {
    if (condition) {
      synchronized (notifyLock) {
        failure = new TestFailure("assertFalse failure");
        
        notifyLock.notifyAll();
      }
      
      throw failure;
    }
  }
  
  /**
   * Verifies that the passed in object is null.  If it is not null an exception is thrown in this 
   * thread, as well as possibly any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param o Object to verify is {@code null}
   */
  public void assertNull(Object o) {
    if (o != null) {
      synchronized (notifyLock) {
        failure = new TestFailure("Object is not null: " + o);
        
        notifyLock.notifyAll();
      }
      
      throw failure;
    }
  }
  
  /**
   * Verifies that the passed in object is not null.  If it is null an exception is thrown in this 
   * thread, as well as possibly any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param o Object to verify is not {@code null}
   */
  public void assertNotNull(Object o) {
    if (o == null) {
      synchronized (notifyLock) {
        failure = new TestFailure("Object is null");
        
        notifyLock.notifyAll();
      }
      
      throw failure;
    }
  }
  
  /**
   * Verifies that the passed in values are equal using the o1.equals(o2) relationship.  If this 
   * check fails an exception is thrown in this thread, as well as any blocking thread waiting at 
   * {@link #waitForTest()}.
   * 
   * @param o1 First object to compare against
   * @param o2 Second object to compare against
   */
  public void assertEquals(Object o1, Object o2) {
    boolean nullMissmatch = (o1 == null && o2 != null) || 
                              (o1 != null && o2 == null);
    if (nullMissmatch || 
        ! ((o1 == null && o2 == null) || o1.equals(o2))) {
      synchronized (notifyLock) {
        failure = new TestFailure(o1 + " != " + o2);
        
        notifyLock.notifyAll();
      }
      
      throw failure;
    }
  }
  
  /**
   * Marks a failure with no cause.  This causes an exception to be thrown in the calling thread, 
   * as well was any blocking thread waiting at {@link #waitForTest()}.
   */
  public void fail() {
    fail("");
  }
  
  /**
   * Marks a failure with a specified message.  This causes an exception to be thrown in the 
   * calling thread, as well was any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param message Message to be provided in failure exception
   */
  public void fail(String message) {
    synchronized (notifyLock) {
      failure = new TestFailure(message);
      
      notifyLock.notifyAll();
    }
    
    throw failure;
  }
  
  /**
   * Marks a failure with a specified throwable cause.  This causes an exception to be thrown in 
   * the calling thread, as well was any blocking thread waiting at {@link #waitForTest()}.
   * 
   * @param cause Throwable cause to be provided in failure exception
   */
  public void fail(Throwable cause) {
    synchronized (notifyLock) {
      failure = new TestFailure(cause);
      
      notifyLock.notifyAll();
    }
    
    throw failure;
  }
  
  /**
   * Exception to represent a failure in a test assertion.
   * 
   * @since 1.0.0
   */
  public static class TestFailure extends RuntimeException {
    private static final long serialVersionUID = -4683332806581392944L;

    private TestFailure(String failureMsg) {
      super(failureMsg);
    }

    private TestFailure(Throwable cause) {
      super(cause);
    }
  }
}
