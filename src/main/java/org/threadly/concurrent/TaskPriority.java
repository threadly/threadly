package org.threadly.concurrent;

/**
 * <p>Priority to go with tasks when being submitted into implementations of 
 * {@link PrioritySchedulerService}.</p>
 * 
 * <p>This priority has nothing to do with the system level thread priority.  Instead this only 
 * represents a priority within the thread pool start a task.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public enum TaskPriority { 
  /**
   * High priority tasks should be executed as soon as possible within the thread pool.
   */
  High, 
  /**
   * Low priority tasks are as the name indicates lower priority compared to high priority task.  
   * It is up to the implementer of the {@link PrioritySchedulerService} as to how this priority 
   * is enforced.  
   * 
   * As a generalization low priority tasks should not be dependent on an accurate execution time. 
   */
  Low,
  /**
   * This priority is never guaranteed to run.  It will only run if there is no 
   * {@link TaskPriority#High} or {@link TaskPriority#Low} available to run.  If there is another 
   * task ready to be ran, this task will be infinitely delayed until there is truly nothing left 
   * to run.
   */
  Starvable;
}