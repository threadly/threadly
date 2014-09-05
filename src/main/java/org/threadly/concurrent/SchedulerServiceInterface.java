package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * <p>This interface adds some more advanced features to a scheduler that are more service 
 * oriented.  Things like a concept of running/shutdown, as well as removing tasks are not always 
 * easy to implement.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public interface SchedulerServiceInterface extends SubmitterSchedulerInterface {
  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * 
   * @param task The original task provided to the executor
   * @return {@code true} if the task was found and removed
   */
  public boolean remove(Runnable task);

  /**
   * Removes the runnable task from the execution queue.  It is possible for the callable to still 
   * run until this call has returned.
   * 
   * @param task The original callable provided to the executor
   * @return {@code true} if the callable was found and removed
   */
  public boolean remove(Callable<?> task);
  
  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown();
}
