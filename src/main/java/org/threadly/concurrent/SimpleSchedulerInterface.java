package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * A simple thread pool that accepts scheduling.
 * 
 * @author jent - Mike Jensen
 */
public interface SimpleSchedulerInterface extends Executor {
  /**
   * Submit a task to run as soon as possible.  There is a 
   * slight increase in load when using submit over execute.  
   * So this should only be used when the future is necessary.
   * 
   * @param task runnable to be executed
   * @return a future to know when the task has completed
   */
  public ExecuteFuture submit(Runnable task);
  
  /**
   * Schedule a task with a given delay.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   */
  public void schedule(Runnable task, long delayInMs);
  
  /**
   * Schedule a task with a given delay.  There is a slight 
   * increase in load when using submitScheduled over schedule.  So 
   * this should only be used when the future is necessary.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public ExecuteFuture submitScheduled(Runnable task, 
                                       long delayInMs);
  
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
