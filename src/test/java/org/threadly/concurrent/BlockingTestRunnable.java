package org.threadly.concurrent;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class BlockingTestRunnable extends TestRunnable {
  private boolean unblocked = false;
  
  @Override
  protected void handleRunStart() throws InterruptedException {
    synchronized (this) {
      while (! unblocked) {
        this.wait();
      }
    }
  }
  
  public void unblock() {
    synchronized (this) {
      unblocked = true;
      
      this.notifyAll();
    }
  }
}
