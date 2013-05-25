package org.threadly.concurrent.lock;

/**
 * This structure allows for more controlled levels of parallism.  It helps 
 * in allowing threads to only lock when their interest are the same.  It 
 * is guaranteed that every key provided will return the same lock.  But it 
 * is not guaranteed that two different keys will not have the same lock.
 * 
 * @author jent - Mike Jensen
 */
public class StripedLock {
  private final int expectedConcurrencyLevel;
  private final LockFactory lockFactory;
  
  public StripedLock(int expectedConcurrencyLevel, LockFactory lockFactory) {
    if (expectedConcurrencyLevel <= 0) {
      throw new IllegalArgumentException("expectedConcurrencyLevel must be > 0: " + expectedConcurrencyLevel);
    } else if (lockFactory == null) {
      throw new IllegalArgumentException("lockFactory can not be null");
    }
    
    this.expectedConcurrencyLevel = expectedConcurrencyLevel;
    this.lockFactory = lockFactory;
  }
}
