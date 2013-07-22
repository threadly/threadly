package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract implementation for classes which limit concurrency 
 * for a parent thread pool.
 * 
 * @author jent - Mike Jensen
 */
public abstract class AbstractThreadPoolLimiter {
  protected final String subPoolName;
  private final int maxConcurrency;
  private final AtomicInteger currentlyRunning;
  
  /**
   * Constructor for abstract class to call into for anyone extending this class.
   * 
   * @param subPoolName name to give threads while tasks running in pool (null to not change thread names)
   * @param maxConcurrency maximum concurrency to allow
   */
  public AbstractThreadPoolLimiter(String subPoolName, int maxConcurrency) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("max concurrency must be at least 1");
    }
    
    if (subPoolName != null) {
      subPoolName = subPoolName.trim();
      
      if (subPoolName.length() == 0) {
        subPoolName = null;
      }
    }
    
    this.subPoolName = subPoolName;
    this.maxConcurrency = maxConcurrency;
    currentlyRunning = new AtomicInteger(0);
  }
  
  /**
   * Constructs a formated name for a given thread for this sub pool.  
   * This only makes sense to call when subPoolName is not null.
   * 
   * @param originalThreadName name of thread before change
   * @return a formated name to change the thread to.
   */
  protected String makeSubPoolThreadName(String originalThreadName) {
    return subPoolName + "[" + originalThreadName + "]";
  }
  
  /**
   * Is block to verify a task can run in a thread safe way.  
   * If this returns true currentlyRunning has been incremented and 
   * it expects the task to run and call handleTaskFinished 
   * when completed.
   * 
   * @return returns true if the task can run
   */
  protected boolean canRunTask() {
    while (true) {  // loop till we have a result
      int currentValue = currentlyRunning.get();
      if (currentValue < maxConcurrency) {
        if (currentlyRunning.compareAndSet(currentValue, 
                                           currentValue + 1)) {
          return true;
        } // else retry in while loop
      } else {
        return false;
      }
    }
  }
  
  /**
   * Will run as many waiting tasks as it can.
   */
  protected abstract void consumeAvailable();
  
  /**
   * Should be called after every task completes.  This decrements 
   * currentlyRunning in a thread safe way, then will run any waiting 
   * tasks which exists.
   */
  protected void handleTaskFinished() {
    currentlyRunning.decrementAndGet();
    
    consumeAvailable(); // allow any waiting tasks to run
  }
}
