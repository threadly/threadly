package org.threadly.test.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.collections.ConcurrentArrayList;

/**
 * <p>Generic runnable implementation that can be used in 
 * unit tests for verifying execution occurred.</p>
 * 
 * <p>This structure is used extensively within threadly's own 
 * unit tests.  Please use those as examples using this class.</p>
 * 
 * @author jent - Mike Jensen
 */
public class TestRunnable extends VirtualRunnable {
  private static final int DEFAULT_TIMEOUT_PER_RUN = 10 * 1000;
  private static final int RUN_CONDITION_POLL_INTERVAL = 20;
  
  private final ConcurrentArrayList<Long> runTime;
  private final AtomicInteger runCount;
  private final AtomicInteger currentRunningCount;
  private final long creationTime;
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
   * This constructor allows the parameter for the runnable to sleep 
   * after handleRunStart was called and before handleRunFinish is called.
   * 
   * @param runTimeInMillis time for runnable to sleep in milliseconds
   */
  public TestRunnable(int runTimeInMillis) {
    setRunDelayInMillis(runTimeInMillis);
    this.runTime = new ConcurrentArrayList<Long>();
    this.runCount = new AtomicInteger(0);
    this.currentRunningCount = new AtomicInteger(0);
    this.ranConcurrent = false;

    this.creationTime = System.currentTimeMillis();
  }
  
  /**
   * Changes the amount of time the runnable will sleep/block 
   * when called.  This change will only be for future run calls 
   * that are not already blocking.
   * 
   * @param runDelayInMillis new amount to wait when run is called
   */
  public void setRunDelayInMillis(int runDelayInMillis) {
    this.runDelayInMillis = runDelayInMillis;
  }
  
  /**
   * Getter for the currently set amount of time the {@link TestRunnable} 
   * will block after handlRunStart and before handleRunFinish is called.
   * 
   * @return current set sleep time in milliseconds
   */
  public int getRunDelayInMillis() {
    return runDelayInMillis;
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
   * Check if this instance has ever been detected to run concurrently.  Keep 
   * in mind that just because this call returns false does not guarantee the 
   * runnable is incapable of running parallel.  It does it's best to detect 
   * a situation where it is started while another instance has not returned 
   * from the run function.  Adding a delay in handleRunStart, handleRunFinish, 
   * or in the constructor increases the chances of detecting concurrency.
   * 
   * @return True f this instance ran in parallel at least once
   */
  public boolean ranConcurrently() {
    return ranConcurrent;
  }
  
  /**
   * Getter to check if the runnable has run exactly once.
   * 
   * @return true if the runnable has been called once
   */
  public boolean ranOnce() {
    return runCount.get() == 1;
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
    
    return runTime.get(runNumber - 1) - getCreationTime();
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
        int finishCount = runCount.get();
        
        if (finishCount < expectedRunCount) {
          return false;
        } else if (finishCount > expectedRunCount) {
          return true;
        } else {  // they are equal
          return currentRunningCount.get() == 0;
        }
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
    int startRunningCount = currentRunningCount.incrementAndGet();
    
    runTime.addLast(System.currentTimeMillis());
    try {
      handleRunStart();
    } catch (InterruptedException e) {
      // ignored
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
  protected void handleRunStart() throws InterruptedException {
    // nothing in default implementation
  }
  
  /**
   * Function to be overloaded by extending classes
   * if more data or operations need to happen at the run point.  
   * 
   * This is the last call to be made in the runnable.  If you want a runtime 
   * exception to get thrown to the caller, it must be thrown from here.
   */
  protected void handleRunFinish() {
    // nothing in default implementation
  }
}
