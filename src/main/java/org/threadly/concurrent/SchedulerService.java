package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * <p>This interface adds some more advanced features to a scheduler that are more service 
 * oriented.  Things like a concept of running/shutdown, as well as removing tasks are not always 
 * easy to implement.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0 (since 2.0.0 as SchedulerServiceInterface)
 */
public interface SchedulerService extends SubmitterScheduler {
  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task (as in a complete 
   * guarantee).  But while this is being invoked, it will reduce the throughput of execution, so 
   * should NOT be used extremely frequently.
   * 
   * For non-recurring tasks using a future and calling 
   * {@link java.util.concurrent.Future#cancel(boolean)} can be a better solution.
   * 
   * @param task The original runnable provided to the executor
   * @return {@code true} if the runnable was found and removed
   */
  public boolean remove(Runnable task);

  /**
   * Removes the callable task from the execution queue.  It is possible for the callable to still 
   * run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task (as in a complete 
   * guarantee).  But while this is being invoked, it will reduce the throughput of execution, so 
   * should NOT be used extremely frequently.
   * 
   * For non-recurring tasks using a future and calling 
   * {@link java.util.concurrent.Future#cancel(boolean)} can be a better solution.
   * 
   * @param task The original callable provided to the executor
   * @return {@code true} if the callable was found and removed
   */
  public boolean remove(Callable<?> task);
  
  /**
   * Call to check how many tasks are currently being executed in this scheduler.
   * 
   * @return current number of running tasks
   */
  public int getActiveTaskCount();
  
  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getQueuedTaskCount();
  
  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown();
}
