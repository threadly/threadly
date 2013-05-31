package org.threadly.concurrent.lock;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This structure allows for more controlled levels of parallelism.  It helps 
 * in allowing threads to only lock when their interest are the same.  It 
 * is guaranteed that every key provided will return the same lock.  But it 
 * is not guaranteed that two different keys will not have the same lock.
 * 
 * @author jent - Mike Jensen
 */
public class StripedLock {
  private final int expectedConcurrencyLevel;
  private final LockFactory lockFactory;
  private final ConcurrentHashMap<Integer, VirtualLock> locks;
  
  /**
   * Constructs a new {@link StripedLock} with a given expected concurrency level.  
   * The higher the concurrency level, the less lock contention will exist, 
   * but more locks will have to be synchronized on and more memory will be 
   * used to store the locks.
   * 
   * @param expectedConcurrencyLevel expected level of parallelism
   * @param lockFactory factory to produce new locks from
   */
  public StripedLock(int expectedConcurrencyLevel, 
                     LockFactory lockFactory) {
    if (expectedConcurrencyLevel <= 0) {
      throw new IllegalArgumentException("expectedConcurrencyLevel must be > 0: " + expectedConcurrencyLevel);
    } else if (lockFactory == null) {
      throw new IllegalArgumentException("lockFactory can not be null");
    }
    
    this.expectedConcurrencyLevel = expectedConcurrencyLevel;
    this.lockFactory = lockFactory;
    this.locks = new ConcurrentHashMap<Integer, VirtualLock>();
  }
  
  /**
   * Getter for the expected concurrency level this class was 
   * constructed with.
   * 
   * @return the constructed level of concurrency
   */
  public int getExpectedConcurrencyLevel() {
    return expectedConcurrencyLevel;
  }
  
  /**
   * Call to get a striped lock for a given key.
   * 
   * @param key to use hashCode() from to determine lock
   * @return consistent VirtualLock for a given key
   */
  public VirtualLock getLock(Object key) {
    if (key == null) {
      return getLock(0);
    } else {
      return getLock(key.hashCode());
    }
  }
  
  /**
   * Call to get a striped lock for a given hash code.
   * 
   * @param hashCode to use to determine which lock to return
   * @return consistent VirtualLock for a given hash code
   */
  public VirtualLock getLock(int hashCode) {
    int lockIndex = Math.abs(hashCode) % expectedConcurrencyLevel;
    VirtualLock result = locks.get(lockIndex);
    if (result == null) {
      result = lockFactory.makeLock();
      VirtualLock putIfAbsentResult = locks.putIfAbsent(lockIndex, result);
      if (putIfAbsentResult != null) {
        result = putIfAbsentResult;
      }
    }
    
    return result;
  }
}
