package org.threadly.concurrent.lock;

/**
 * VirtualLock interface so that this item can be easily injected.
 * 
 * @author jent - Mike Jensen
 */
public abstract class VirtualLock {
  /**
   * Equivalent to Object.wait() but for injectable VirtualLock implementation.
   * 
   * @throws InterruptedException Thrown when thread is interrupted
   */
  public abstract void await() throws InterruptedException;
  
  /**
   * Equivalent to Object.wait(long) but for injectable VirtualLock implementation.
   * 
   * @param waitTimeInMs time to wait on lock
   * @throws InterruptedException Thrown when thread is interrupted
   */
  public abstract void await(long waitTimeInMs) throws InterruptedException;
  
  /**
   * Equivalent to Object.notify() but for injectable VirtualLock implementation.
   */
  public abstract void signal();
  
  /**
   * Equivalent to Object.notifyAll() but for injectable VirtualLock implementation.
   */
  public abstract void signalAll();

  /**
   * Equivalent to Thread.sleep() but for injectable VirtualLock implementation.
   * 
   * @param timeInMs Time in milliseconds to wait before continuing execution
   * @throws InterruptedException Thrown when thread is interrupted
   */
  public abstract void sleep(long timeInMs) throws InterruptedException;
  
  /**
   * Calls .await() but ignores any InterruptedExceptions that may be thrown.
   */
  public void awaitUninterruptibly() {
    awaitUninterruptibly(Long.MAX_VALUE);
  }
  
  /**
   * Calls .await(long) but ignores any InterruptedExceptions that may be thrown.
   * 
   * @param waitTimeInMs Time to wait on lock
   */
  public void awaitUninterruptibly(long waitTimeInMs) {
    boolean done = false;
    while (! done) {
      try {
        await(waitTimeInMs);
        done = true;
      } catch (InterruptedException e) {
        // ignored
      }
    }
  }

  /**
   * Calls .sleep(long) but ignores any InterruptedExceptions that may be thrown.
   * 
   * @param timeInMs time in milliseconds to wait before resuming execution
   */
  public void sleepUninterruptibly(long timeInMs) {
    boolean done = false;
    while (! done) {
      try {
        sleep(timeInMs);
        done = true;
      } catch (InterruptedException e) {
        // ignored
      }
    }
  }
}
