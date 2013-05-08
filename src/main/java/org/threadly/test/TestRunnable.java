package org.threadly.test;

import org.threadly.concurrent.VirtualRunnable;

/**
 * Runnable that can be used in unit tests for verifying execution occurred
 * 
 * @author jent - Mike Jensen
 */
public class TestRunnable extends VirtualRunnable {
  private final TestCondition runCondition;
  private final long creationTime;
  private volatile int expectedRunCount;
  private volatile long firstRunTime;
  private volatile int runCount;

  /**
   * Constructs a new runnable for unit testing
   */
  public TestRunnable() {
    creationTime = System.currentTimeMillis();
    firstRunTime = -1;
    expectedRunCount = -1;
    runCount = 0;
    runCondition = new TestCondition() {
      @Override
      public boolean get() {
        return runCount >= expectedRunCount;
      }
    };
  }
  
  /**
   * @return True if the runnable has been called once
   */
  public boolean ranOnce() {
    return runCount == 1;
  }
  
  /**
   * @return The number of times the run function has been called
   */
  public int getRunCount() {
    return runCount;
  }
  
  /**
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillFirstRun() {
    blockTillRun();
    
    return firstRunTime - creationTime;
  }
  
  /**
   * Blocks until run has been called at least once
   */
  public void blockTillRun() {
    blockTillRun(1000, 1);
  }

  /**
   * Blocks until run has been called at least once
   * 
   * @param timeout time to wait for run to be called
   */
  public void blockTillRun(int timeout) {
    blockTillRun(timeout, 1);
  }
  
  /**
   * Blocks until run has been called the provided qty of times
   * 
   * @param timeout time to wait for run to be called
   * @param expectedRunCount run count to wait for
   */
  public void blockTillRun(int timeout, int expectedRunCount) {
    this.expectedRunCount = expectedRunCount;
    runCondition.blockTillTrue(timeout, 20);
  }
  
  @Override
  public final void run() {
    runCount++;
    if (firstRunTime < 0) {
      firstRunTime = System.currentTimeMillis();
    }
    
    try {
      handleRun();
    } catch (InterruptedException e) {
      // ignored
    }
  }
  
  /**
   * Function to be overloaded by extending classes
   * if more data or operations need to happen at the 
   * run point.  
   * 
   * This is also the last call to be made in the runnable,
   * so if you were to simulate an exception, this gives 
   * you the opportunity to do so.
   * @throws InterruptedException - only InterruptedExceptions will be swallowed
   */
  public void handleRun() throws InterruptedException {
    // nothing in default implementation
  }
}
