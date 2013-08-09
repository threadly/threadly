package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * A simple thread pool that accepts scheduling.
 * 
 * @author jent - Mike Jensen
 */
public interface SimpleSchedulerInterface extends Executor {
  /**
   * Schedule a one time task with a given delay.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   */
  public void schedule(Runnable task, long delayInMs);
  
  /**
   * Schedule a recurring task to run.  The recurring delay time will be
   * from the point where execution finished.
   * 
   * @param task runnable to be executed.
   * @param initialDelay delay in milliseconds until first run.
   * @param recurringDelay delay in milliseconds for running task after last finish.
   */
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay);

  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * @return true if thread pool is running
   */
  public boolean isShutdown();
}
