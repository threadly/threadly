package org.threadly.concurrent.lock;

/**
 * <p>Implementation of {@link LockFactory} which produces 
 * native (blocking) locks.  See {@link NativeLock}.</p>
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
