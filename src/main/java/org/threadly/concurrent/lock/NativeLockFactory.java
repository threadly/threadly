package org.threadly.concurrent.lock;

/**
 * Factory to be injected that produces native locks.
 * 
 * @author jent - Mike Jensen
 */
public class NativeLockFactory implements LockFactory {
  /**
   * Constructs a new {@link NativeLockFactory}.
   */
  public NativeLockFactory() {
  }

  @Override
  public VirtualLock makeLock() {
    return new NativeLock();
  }

  @Override
  public boolean isNativeLockFactory() {
    return true;
  }
}
