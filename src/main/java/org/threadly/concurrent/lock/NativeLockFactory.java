package org.threadly.concurrent.lock;

/**
 * Factory to be injected that produces native locks.
 * 
 * @author jent - Mike Jensen
 */
public class NativeLockFactory implements LockFactory {
  /**
   * Constructs a new native lock factory
   */
  public NativeLockFactory() {
  }

  @Override
  public VirtualLock makeLock() {
    return new NativeLock();
  }
}
