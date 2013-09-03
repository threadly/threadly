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
  
  public boolean isNativeLockFactory();
}
