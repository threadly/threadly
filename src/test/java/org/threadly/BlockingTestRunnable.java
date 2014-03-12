package org.threadly;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class BlockingTestRunnable extends TestRunnable {
  private boolean unblocked = false;
  
  @Override
  public void handleRunStart() throws InterruptedException {
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
