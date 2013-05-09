package org.threadly.test;

import org.threadly.concurrent.VirtualRunnable;
import java.util.LinkedList;

/**
 * Generic runnable implementation that can be used in 
 * unit tests for verifying execution occurred
 * 
 * @author jent - Mike Jensen
 */
public class TestRunnable extends VirtualRunnable {
  private static final int DEFAULT_TIMEOUT_PER_RUN = 2000;
  
  private final TestCondition runCondition;
  private final long creationTime;
  private volatile int expectedRunCount;
  private volatile LinkedList<Long> runTime;
  private volatile int runCount;

  /**
   * Constructs a new runnable for unit testing
   */
  public TestRunnable() {
    creationTime = System.currentTimeMillis();
    runTime = new LinkedList<Long>();
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
   * This function blocks till 
   * 
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillFirstRun() {
    return getDelayTillRun(1);
  }
  
  /**
   * This function blocks till the run provided, and 
   * then gets the time between creation and a given run.
   * 
   * @param runNumber the run count to get delay to
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillRun(int runNumber) {
    return getDelayTillRun(DEFAULT_TIMEOUT_PER_RUN * runNumber, runNumber);
  }
  
  /**
   * This function blocks till the run provided, and 
   * then gets the time between creation and a given run.
   * 
   * @param timeout timeout to wait for given run count to finish
   * @param runNumber the run count to get delay to
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillRun(int timeout, int runNumber) {
    blockTillRun(timeout, runNumber);
    
    return runTime.get(runNumber - 1) - creationTime;
  }
  
  /**
   * Blocks until run has been called at least once
   */
  public void blockTillRun() {
    blockTillRun(DEFAULT_TIMEOUT_PER_RUN, 1);
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
    try {
      handleRun();
    } catch (InterruptedException e) {
      // ignored
    } finally {
      runTime.add(System.currentTimeMillis());
      runCount++;
    }
  }
  
  /**
   * Function to be overloaded by extending classes
   * if more data or operations need to happen at the 
   * run point.  
   * 
   * This is also the first call to be made in the runnable,
   * but all necessary TestRunnable actions are in a finally block
   * so it is safe to throw any exceptions necessary here.
   * 
   * @throws InterruptedException only InterruptedExceptions will be swallowed
   */
  public void handleRun() throws InterruptedException {
    // nothing in default implementation
  }
}
