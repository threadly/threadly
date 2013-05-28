package org.threadly.test.concurrent;

import java.util.concurrent.Executor;

import org.threadly.test.concurrent.lock.TestableLock;

/**
 * An interface for executor which can handle VirtualRunnable's and VirtualCallables 
 * in a testing situation.
 * 
 * @author jent - Mike Jensen
 */
public interface TestableExecutor extends Executor {
  /**
   * should only be called from TestableVirtualLock.
   * 
   * @param lock lock referencing calling into scheduler
   * @throws InterruptedException thrown if the thread is interrupted while blocking
   */
  public void waiting(TestableLock lock) throws InterruptedException;
  
  /**
   * should only be called from TestableVirtualLock.
   * 
   * @param lock lock referencing calling into scheduler
   * @param waitTimeInMs time to wait on lock
   * @throws InterruptedException thrown if the thread is interrupted while blocking
   */
  public void waiting(final TestableLock lock, 
                      long waitTimeInMs) throws InterruptedException;

  /**
   * should only be called from TestableVirtualLock.
   * 
   * @param lock lock referencing calling into scheduler
   */
  public void signal(TestableLock lock);

  /**
   * should only be called from TestableVirtualLock.
   * 
   * @param lock lock referencing calling into scheduler
   */
  public void signalAll(TestableLock lock);

  /**
   * should only be called from TestableVirtualLock or 
   * the running thread inside the scheduler.
   * 
   * @param sleepTime time for thread to sleep
   * @throws InterruptedException thrown if the thread is interrupted while sleeping
   */
  public void sleep(long sleepTime) throws InterruptedException;
}
