package org.threadly.test.concurrent;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.util.Clock;

/**
 * <p>Generic runnable implementation that can be used in unit tests for verifying execution 
 * occurred.</p>
 * 
 * <p>This structure is used extensively within threadly's own unit tests.  Please use those as 
 * examples using this class.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class TestRunnable implements Runnable {
  protected static final int DEFAULT_TIMEOUT_PER_RUN = 10 * 1000;
  protected static final int RUN_CONDITION_POLL_INTERVAL = 20;

  protected final long creationForwardProgressingMillis;
  private final ConcurrentArrayList<Long> runTime;
  private final AtomicInteger runCount;
  private final AtomicInteger currentRunningCount;
  private volatile int runDelayInMillis;
  private volatile boolean ranConcurrent;

  /**
   * Constructs a new runnable for unit testing.
   */
  public TestRunnable() {
    this(0);
  }

  /**
   * Constructs a new runnable for unit testing.  
   * 
   * This constructor allows the parameter for the runnable to sleep after {@link #handleRunStart()} 
   * was called and before {@link #handleRunFinish()} is called.
   * 
   * @param runTimeInMillis time for runnable to sleep in milliseconds
   */
  public TestRunnable(int runTimeInMillis) {
    this.creationForwardProgressingMillis = Clock.accurateForwardProgressingMillis();
    
    setRunDelayInMillis(runTimeInMillis);
    this.runTime = new ConcurrentArrayList<Long>(0, 1);
    this.runCount = new AtomicInteger(0);
    this.currentRunningCount = new AtomicInteger(0);
    this.ranConcurrent = false;
  }
  
  /**
   * Changes the amount of time the runnable will sleep/block when called.  This change will only 
   * be for future run calls that are not already blocking.
   * 
   * @param runDelayInMillis new amount to wait when run is called
   */
  public void setRunDelayInMillis(int runDelayInMillis) {
    this.runDelayInMillis = runDelayInMillis;
  }
  
  /**
   * Getter for the currently set amount of time the {@link TestRunnable} will block after 
   * {@link #handleRunStart()} and before {@link #handleRunFinish()} is called.
   * 
   * @return current set sleep time in milliseconds
   */
  public int getRunDelayInMillis() {
    return runDelayInMillis;
  }
  
  /**
   * Returns the millis returned from {@link Clock#accurateForwardProgressingMillis()}, that was 
   * recorded when this runnable was constructed.
   * 
   * @return Forward progressing time in milliseconds {@link TestRunnable} was constructed
   */
  public long getCreationTime() {
    return creationForwardProgressingMillis;
  }
  
  /**
   * Check if this instance has ever been detected to run concurrently.  Keep in mind that just 
   * because this call returns false does not guarantee the runnable is incapable of running 
   * parallel.  It does it's best to detect a situation where it is started while another instance 
   * has not returned from the run function.  Adding a delay in {@link #handleRunStart()}, 
   * {@link #handleRunFinish()}, or in the constructor increases the chances of detecting 
   * concurrency.
   * 
   * @return True if this instance ran in parallel at least once
   */
  public boolean ranConcurrently() {
    return ranConcurrent;
  }
  
  /**
   * Getter to check if the runnable has run exactly once.
   * 
   * @return {@code true} if the runnable has been called once
   */
  public boolean ranOnce() {
    return runCount.get() == 1;
  }
  
  /**
   * Check to see if the run function has started but has not completed returned yet.
   * 
   * @return True if the runnable's run function is being called currently.
   */
  public boolean isRunning() {
    return currentRunningCount.get() != 0;
  }
  
  /**
   * Getter for the number of times the run function has completed.
   * 
   * @return The number of times the run function has been called
   */
  public int getRunCount() {
    return runCount.get();
  }
  
  /**
   * This function blocks till the first run completes then will return the time until the first 
   * run started it's call.
   * 
   * @return The amount of time between construction and run being called
   */
  public long getDelayTillFirstRun() {
    return getDelayTillRun(1);
  }
  
  /**
   * This function blocks till the run provided, and then gets the time between creation and a 
   * given run.
   * 
   * @param runNumber the run count to get delay to
   * @return The amount of time between construction and run being called
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
   * @return The amount of time between construction and run being called
   */
  public long getDelayTillRun(int runNumber, int timeout) {
    blockTillFinished(timeout, runNumber);
    
    Collections.sort(runTime);
    return runTime.get(runNumber - 1) - getCreationTime();
  }
  
  /**
   * Blocks until run has completed at least once.  Will throw an exception if runnable does not 
   * run within 10 seconds.
   */
  public void blockTillFinished() {
    blockTillFinished(DEFAULT_TIMEOUT_PER_RUN, 1);
  }

  /**
   * Blocks until run has completed at least once.
   * 
   * @param timeout time to wait for run to be called before throwing exception
   */
  public void blockTillFinished(int timeout) {
    blockTillFinished(timeout, 1);
  }
  
  /**
   * Blocks until run completed been called the provided quantity of times.
   * 
   * @param timeout time to wait for run to be called before throwing exception
   * @param expectedRunCount run count to wait for
   */
  public void blockTillFinished(int timeout, 
                                int expectedRunCount) {
    final int blockRunCount = expectedRunCount;
    
    new TestCondition() {
      @Override
      public boolean get() {
        int finishCount = runCount.get();
        
        if (finishCount < blockRunCount) {
          return false;
        } else if (finishCount > blockRunCount) {
          return true;
        } else {  // they are equal
          return currentRunningCount.get() == 0;
        }
      }
    }.blockTillTrue(timeout, RUN_CONDITION_POLL_INTERVAL);
  }
  
  /**
   * Blocks until run has been called at least once.  Will throw an exception if run is not called 
   * within 10 seconds.
   */
  public void blockTillStarted() {
    blockTillStarted(DEFAULT_TIMEOUT_PER_RUN);
  }

  /**
   * Blocks until run has been called at least once.
   * 
   * @param timeout time to wait for run to be called before throwing exception
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
    int startRunningCount = currentRunningCount.incrementAndGet();
    
    runTime.addLast(Clock.accurateForwardProgressingMillis());
    try {
      handleRunStart();
    } catch (InterruptedException e) {
      // ignored, just reset status
      Thread.currentThread().interrupt();
    } finally {
      if (runDelayInMillis > 0) {
        TestUtils.sleep(runDelayInMillis);
      }
      
      runCount.incrementAndGet();
      
      try {
        handleRunFinish();
      } finally {
        ranConcurrent = currentRunningCount.decrementAndGet() != 0 || // must be first to ensure decrement is called
                          ranConcurrent || startRunningCount != 1;
      }
    }
  }
  
  /**
   * Function to be overloaded by extending classes if more data or operations need to happen at 
   * the run point.  
   * 
   * This is also the first call to be made in the runnable, but all necessary 
   * {@link TestRunnable} actions are in a finally block so it is safe to throw any exceptions 
   * necessary here.
   * 
   * @throws InterruptedException only InterruptedExceptions will be swallowed
   */
  public void handleRunStart() throws InterruptedException {
    // only public to include in javadocs, otherwise could be protected
    // nothing in default implementation
  }
  
  /**
   * Function to be overloaded by extending classes if more data or operations need to happen at 
   * the run point.  
   * 
   * This is the last call to be made in the runnable.  If you want a {@link RuntimeException} to 
   * get thrown to the caller, it must be thrown from here.
   */
  public void handleRunFinish() {
    // only public to include in javadocs, otherwise could be protected
    // nothing in default implementation
  }
}
