package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * <p>An interface for a simple thread pool that accepts scheduled and recurring tasks.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
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
   * Schedule a recurring task to run.  The recurring delay time will be from the point where 
   * execution finished.  So the execution frequency is the {@code recurringDelay + runtime} for 
   * the provided task.
   * 
   * Unlike {@link java.util.concurrent.ScheduledExecutorService} if the task throws an exception, 
   * subsequent executions are NOT suppressed or prevented.  So if the task throws an exception on 
   * every run, the task will continue to be executed at the provided recurring delay (possibly 
   * throwing an exception on each execution).
   * 
   * @param task runnable to be executed.
   * @param initialDelay delay in milliseconds until first run.
   * @param recurringDelay delay in milliseconds for running task after last finish.
   */
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay);
}
