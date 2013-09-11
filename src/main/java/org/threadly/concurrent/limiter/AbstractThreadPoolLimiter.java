package org.threadly.concurrent.limiter;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.future.FutureFuture.TaskCanceler;

/**
 * Abstract implementation for classes which limit concurrency 
 * for a parent thread pool.
 * 
 * @author jent - Mike Jensen
 */
abstract class AbstractThreadPoolLimiter {
  protected final int maxConcurrency;
  protected final String subPoolName;
  private final AtomicInteger currentlyRunning;
  
  /**
   * Constructor for abstract class to call into for anyone extending this class.
   * 
   * @param maxConcurrency maximum concurrency to allow
   * @param subPoolName name to give threads while tasks running in pool (null to not change thread names)
   */
  public AbstractThreadPoolLimiter(int maxConcurrency, String subPoolName) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("max concurrency must be at least 1");
    }
    
    this.maxConcurrency = maxConcurrency;
    
    if (subPoolName != null) {
      subPoolName = subPoolName.trim();
      
      if (subPoolName.length() == 0) {
        subPoolName = null;
      }
    }
    this.subPoolName = subPoolName;
    
    currentlyRunning = new AtomicInteger(0);
  }
  
  /**
   * Call to check what the maximum concurrency this limiter will allow.
   * 
   * @return maximum concurrent tasks to be run
   */
  public int getMaxConcurrency() {
    return maxConcurrency;
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
  
  /**
   * Generic wrapper for runnables which are used within the limiters.
   * This wrapper ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class LimiterRunnableWrapper extends VirtualRunnable
                                         implements TaskCanceler {
    private final Runnable runnable;
    private final AtomicInteger runStatus;  // 0 = not started, -1 = canceled, 1 = running
    
    public LimiterRunnableWrapper(Runnable runnable) {
      this.runnable = runnable;
      runStatus = new AtomicInteger(0);
    }
    
    protected void doAfterRunTasks() {
      // nothing in the default implementation
    }
    
    @Override
    public void run() {
      if (! runStatus.compareAndSet(0, 1)) {
        return;
      }
      Thread currentThread = null;
      String originalThreadName = null;
      if (subPoolName != null) {
        currentThread = Thread.currentThread();
        originalThreadName = currentThread.getName();
        
        currentThread.setName(makeSubPoolThreadName(originalThreadName));
      }
      
      try {
        if (factory != null && 
            runnable instanceof VirtualRunnable) {
          VirtualRunnable vr = (VirtualRunnable)runnable;
          vr.run(factory);
        } else {
          runnable.run();
        }
      } finally {
        try {
          doAfterRunTasks();
        } finally {
          runStatus.compareAndSet(1, 0);
          try {
            handleTaskFinished();
          } finally {
            if (subPoolName != null) {
              currentThread.setName(originalThreadName);
            }
          }
        }
      }
    }

    @Override
    public boolean cancel() {
      return runStatus.compareAndSet(0, -1);
    }
  }
}
