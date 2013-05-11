package org.threadly.concurrent.lock;

import org.threadly.concurrent.TestablePriorityScheduler;

/**
 * Lock which is used for a testable scheduler which may not want
 * threads to be blocked.
 * 
 * @author jent
 */
public class TestableLock extends VirtualLock {
  private final TestablePriorityScheduler scheduler;
  
  /**
   * Constructs a new TestableLock with a testable scheduler 
   * to default to for implementation.
   * 
   * @param scheduler Scheduler which lock operations default to.
   */
  public TestableLock(TestablePriorityScheduler scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler for lock to deffer to");
    }
    
    this.scheduler = scheduler;
  }

  @Override
  public void await() throws InterruptedException {
    scheduler.waiting(this);
  }

  @Override
  public void await(long waitTimeInMs) throws InterruptedException {
    scheduler.waiting(this, waitTimeInMs);
  }

  @Override
  public void signal() {
    scheduler.signal(this);
  }

  @Override
  public void signalAll() {
    scheduler.signalAll(this);
  }

  @Override
  public void sleep(long timeInMs) throws InterruptedException {
    scheduler.sleep(timeInMs);
  }
}
