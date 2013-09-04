package org.threadly.concurrent.lock;

/**
 * Factory that can be injected to produce locks that make sense
 * for a given thread pool.
 * 
 * @author jent - Mike Jensen
 */
public interface LockFactory {
  /**
   * Produces a new lock.
   * 
   * @return new {@link VirtualLock}
   */
  public VirtualLock makeLock();
  
  /**
   * Call to check if the lock factory produces blocking native locks.  
   * Or if it defaults to a test implementation.
   * 
   * @return true if the factory builds native/blocking locks
   */
  public boolean isNativeLockFactory();
}
