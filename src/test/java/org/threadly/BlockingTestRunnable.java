package org.threadly;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class BlockingTestRunnable extends TestRunnable {
  private volatile boolean unblocked = false;
  
  @Override
  public void handleRunStart() throws InterruptedException {
    synchronized (this) {
      while (! unblocked) {
        this.wait();
      }
    }
  }
  
  public boolean isUnblocked() {
    return unblocked;
  }
  
  public void unblock() {
    synchronized (this) {
      unblocked = true;
      
      this.notifyAll();
    }
  }
}
