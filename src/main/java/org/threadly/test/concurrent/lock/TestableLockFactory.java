package org.threadly.test.concurrent.lock;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestablePriorityScheduler;

/**
 * A lock factory that works in conjunction with a testable scheduler.
 * 
 * @author jent - MIke Jensen
 */
public class TestableLockFactory implements LockFactory {
  private final TestablePriorityScheduler scheduler;
  
  /**
   * Constructs a new {@link TestableLockFactory} that can be injected during unit testing.
   * 
   * @param scheduler Scheduler to be provided to TestableLocks which are produced
   */
  public TestableLockFactory(TestablePriorityScheduler scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler to make locks for");
    }
    
    this.scheduler = scheduler;
  }

  @Override
  public VirtualLock makeLock() {
    return new TestableLock(scheduler);
  }

  @Override
  public boolean isNativeLockFactory() {
    return false;
  }
}
