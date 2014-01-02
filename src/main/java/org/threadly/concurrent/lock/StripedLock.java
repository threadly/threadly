package org.threadly.concurrent.lock;

import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>This structure allows for more controlled levels of parallelism.  It helps 
 * in allowing threads to only lock when their interest are the same.  It 
 * is guaranteed that every key provided will return the same lock.  But it 
 * is not guaranteed that two different keys will not have the same lock.</p>
 * 
 * <p>Currently this class only provides objects that should be synchronized on.  
 * Compared to java.util.concurrent.locks.Lock objects which have .lock() and 
 * .unlock() functionality.  This choice was primarily because of the way the 
 * internals of threadly work.</p>
 * 
 * @author jent - Mike Jensen
 */
public class StripedLock {
  private final int expectedConcurrencyLevel;
  private final ConcurrentHashMap<Integer, Object> locks;
  
  /**
   * Constructs a new {@link StripedLock} with a given expected concurrency level.  
   * The higher the concurrency level, the less lock contention will exist, 
   * but more locks will have to be synchronized on and more memory will be 
   * used to store the locks.
   * 
   * @param expectedConcurrencyLevel expected level of parallelism
   */
  public StripedLock(int expectedConcurrencyLevel) {
    if (expectedConcurrencyLevel <= 0) {
      throw new IllegalArgumentException("expectedConcurrencyLevel must be > 0: " + expectedConcurrencyLevel);
    }
    
    this.expectedConcurrencyLevel = expectedConcurrencyLevel;
    this.locks = new ConcurrentHashMap<Integer, Object>();
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
   * Call to get a lock object for a given key.
   * 
   * @param key to use hashCode() from to determine lock
   * @return consistent Object for a given key
   */
  public Object getLock(Object key) {
    if (key == null) {
      return getLock(0);
    } else {
      return getLock(key.hashCode());
    }
  }
  
  /**
   * Call to get a lock object for a given hash code.
   * 
   * @param hashCode to use to determine which lock to return
   * @return consistent Object for a given hash code
   */
  public Object getLock(int hashCode) {
    int lockIndex = Math.abs(hashCode) % expectedConcurrencyLevel;
    Object result = locks.get(lockIndex);
    if (result == null) {
      result = new Object();
      Object putIfAbsentResult = locks.putIfAbsent(lockIndex, result);
      if (putIfAbsentResult != null) {
        result = putIfAbsentResult;
      }
    }
    
    return result;
  }
}
