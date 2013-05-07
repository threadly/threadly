package org.threadly.concurrent;

/**
 * This interface represents schedulers which can not only execute
 * and schedule tasks, but run based off a given priority as well.
 * 
 * @author jent - Mike Jensen
 */
public interface PrioritySchedulerInterface extends SimpleSchedulerInterface {
  /**
   * Executes the task as soon as possible with the given priority.
   * 
   * @param task Task to execute
   * @param priority Priority for task
   */
  public void execute(Runnable task, TaskPriority priority);
  
  /**
   * Schedule a task with a given delay and a specified priority.
   * 
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   * @param priority Priority to give task for execution
   */
  public void schedule(Runnable task, long delayInMs, TaskPriority priority);

  /**
   * Schedule a recurring task to run and a provided priority.  The recurring 
   * delay time will be from the point where execution finished.
   * 
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   * @param priority Priority for task to run at
   */
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority);
  
  /**
   * Get the default priority for the scheduler.
   * 
   * @return the set default task priority
   */
  public TaskPriority getDefaultPriority();
}
