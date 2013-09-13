package org.threadly.test.concurrent;

import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.collections.ConcurrentArrayList;

/**
 * Generic runnable implementation that can be used in 
 * unit tests for verifying execution occurred.
 * 
 * @author jent - Mike Jensen
 */
public class TestRunnable extends VirtualRunnable {
  private static final int DEFAULT_TIMEOUT_PER_RUN = 10 * 1000;
  private static final int RUN_CONDITION_POLL_INTERVAL = 20;
  
  private final long creationTime;
  private final ConcurrentArrayList<Long> runTime;
  private volatile int runCount;

  /**
   * Constructs a new runnable for unit testing.
   */
  public TestRunnable() {
    creationTime = System.currentTimeMillis();
    runTime = new ConcurrentArrayList<Long>();
    runCount = 0;
  }
  
  /**
   * Call to get the time recorded when the runnable was constructed.
   * 
   * @return time in milliseconds object was constructed
   */
  public long getCreationTime() {
    return creationTime;
  }
  
  /**
   * Getter to check if the runnable has run exactly once.
   * 
   * @return true if the runnable has been called once
   */
  public boolean ranOnce() {
    return runCount == 1;
  }
  
  /**
   * Getter for the number of times the run function has completed.
   * 
   * @return The number of times the run function has been called
   */
  public int getRunCount() {
    return runCount;
  }
  
  /**
   * This function blocks till the first run completes then
   * will return the time until the first run started it's call.
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
   * 
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillRun(int runNumber) {
    return getDelayTillRun(runNumber, DEFAULT_TIMEOUT_PER_RUN * runNumber);
  }
  
  /**
   * This function blocks till the run provided, and 
   * then gets the time between creation and a given run.
   * 
   * @param runNumber the run count to get delay to
   * @param timeout timeout to wait for given run count to finish
   * 
   * @return the amount of time between construction and run being called
   */
  public long getDelayTillRun(int runNumber, int timeout) {
    blockTillFinished(timeout, runNumber);
    
    return runTime.get(runNumber - 1) - creationTime;
  }
  
  /**
   * Blocks until run has completed at least once.
   */
  public void blockTillFinished() {
    blockTillFinished(DEFAULT_TIMEOUT_PER_RUN, 1);
  }

  /**
   * Blocks until run has completed at least once.
   * 
   * @param timeout time to wait for run to be called
   */
  public void blockTillFinished(int timeout) {
    blockTillFinished(timeout, 1);
  }
  
  /**
   * Blocks until run completed been called the provided qty of times.
   * 
   * @param timeout time to wait for run to be called
   * @param expectedRunCount run count to wait for
   */
  public void blockTillFinished(int timeout, 
                                final int expectedRunCount) {
    new TestCondition() {
      @Override
      public boolean get() {
        return runCount >= expectedRunCount;
      }
    }.blockTillTrue(timeout, RUN_CONDITION_POLL_INTERVAL);
  }
  
  /**
   * Blocks until run has been called at least once.
   */
  public void blockTillStarted() {
    blockTillStarted(DEFAULT_TIMEOUT_PER_RUN);
  }

  /**
   * Blocks until run has been called at least once.
   * 
   * @param timeout time to wait for run to be called
   */
  public void blockTillStarted(int timeout) {
    new TestCondition() {
      @Override
      public boolean get() {
        return ! runTime.isEmpty();
      }
    }.blockTillTrue(timeout, RUN_CONDITION_POLL_INTERVAL);
  }
  
  @Override
  public final void run() {
    runTime.addLast(System.currentTimeMillis());
    synchronized (this) {
      try {
        handleRunStart();
      } catch (InterruptedException e) {
        // ignored
      } finally {
        runCount++;
        
        handleRunFinish();
      }
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
  public void handleRunStart() throws InterruptedException {
    // nothing in default implementation
  }
  
  /**
   * Function to be overloaded by extending classes
   * if more data or operations need to happen at the run point.  
   * 
   * This is the last call to be made in the runnable.  If you want a runtime 
   * exception to get thrown to the caller, it must be thrown from here.
   */
  public void handleRunFinish() {
    // nothing in default implementation
  }
}
