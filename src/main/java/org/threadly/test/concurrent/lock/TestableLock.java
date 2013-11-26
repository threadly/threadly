package org.threadly.test.concurrent.lock;

import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.TestableExecutor;

/**
 * <p>Lock which is used for a testable scheduler which may not want
 * threads to be blocked.</p>
 * 
 * @author jent - Mike Jensen
 */
public class TestableLock extends VirtualLock {
  private final TestableExecutor executor;
  
  /**
   * Constructs a new {@link TestableLock} with a testable scheduler 
   * to default to for implementation.
   * 
   * @param executor Scheduler which lock operations default to.
   */
  public TestableLock(TestableExecutor executor) {
    if (executor == null) {
      throw new IllegalArgumentException("Must provide executor for lock to deffer to");
    }
    
    this.executor = executor;
  }

  @Override
  public void await() throws InterruptedException {
    executor.handleWaiting(this);
  }

  @Override
  public void await(long waitTimeInMs) throws InterruptedException {
    executor.handleWaiting(this, waitTimeInMs);
  }

  @Override
  public void signal() {
    executor.handleSignal(this);
  }

  @Override
  public void signalAll() {
    executor.handleSignalAll(this);
  }

  @Override
  public void sleep(long timeInMs) throws InterruptedException {
    executor.handleSleep(timeInMs);
  }
}
