package org.threadly.concurrent.lock;

/**
 * Lock that defaults to the native java calls for wait, signal, and sleep.
 * This is what is used for normal operation.
 * 
 * @author jent - Mike Jensen
 */
public class NativeLock extends VirtualLock {
  /**
   * Constructs a new {@link NativeLock}.
   */
  public NativeLock() {
  }
  
  @Override
  public void await() throws InterruptedException {
    this.wait();
  }
  
  @Override
  public void await(long waitTimeInMs) throws InterruptedException {
    this.wait(waitTimeInMs);
  }

  @Override
  public void signal() {
    this.notify();
  }

  @Override
  public void signalAll() {
    this.notifyAll();
  }

  @Override
  public void sleep(long timeInMs) throws InterruptedException {
    Thread.sleep(timeInMs);
  }
}
