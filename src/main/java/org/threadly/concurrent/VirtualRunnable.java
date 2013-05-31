package org.threadly.concurrent;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * This class helps assist in making concurrent code testable.
 * This class is not strictly required, but it makes the {@link org.threadly.test.concurrent.TestablePriorityScheduler}
 * a drop in replacement instead of having to pass a LockFactory into your code.
 * 
 * The alternative to using this class would be to pass a LockFactory into your code.
 * But in addition if the runnable needs to sleep, it must do it on the scheduler or locks. 
 * 
 * @author jent - Mike Jensen
 */
public abstract class VirtualRunnable implements Runnable {
  protected LockFactory factory = null;
  
  /**
   * This is the run call that will be called for schedulers aware of 
   * VirtualRunnable (currently only TestablePriorityScheduler).  This
   * lets us inject a lock factory that works for the given scheduler.
   *  
   * @param factory factory to use while running this runnable
   */
  public void run(LockFactory factory) {
    this.factory = factory;
    
    try {
      run();
    } finally {
      this.factory = null;
    }
  }
  
  /**
   * Returns a virtual lock for the runnable that makes sense for the 
   * processing thread pool.
   * 
   * @return VirtualLock to synchronize on and use with pleasure
   */
  protected VirtualLock makeLock() {
    if (factory == null) {
      return new NativeLock();
    } else {
      return factory.makeLock();
    }
  }
  
  /**
   * Alternative to Thread.sleep().  This call defaults to the native 
   * version or uses a method that makes sense for the running thread pool.
   * 
   * You have to use this instead of Thread.sleep() or you will block the 
   * TestablePriorityScheduler.
   * 
   * @param sleepTime Time to pause thread
   * @throws InterruptedException
   */
  protected void sleep(long sleepTime) throws InterruptedException {
    if (factory == null) {
      Thread.sleep(sleepTime);
    } else {
      factory.makeLock().sleep(sleepTime);
    }
  }
}
