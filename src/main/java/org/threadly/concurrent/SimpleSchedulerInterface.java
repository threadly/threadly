package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * @author jent - Mike Jensen
 */
public interface SimpleSchedulerInterface extends Executor {
  /**
   * Schedule a recurring task to run.  The recurring delay time will be
   * from the point where execution finished.
   * 
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   */
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay);
  
  /**
   * Schedule a task with a given delay.
   * 
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  public void schedule(Runnable task, 
                       long delayInMs);
}
