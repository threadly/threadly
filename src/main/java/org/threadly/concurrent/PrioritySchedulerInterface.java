package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * <p>This interface represents schedulers which can not only execute and schedule tasks, but run 
 * based off a given priority as well.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public interface PrioritySchedulerInterface extends SchedulerServiceInterface {
  /**
   * Executes the task as soon as possible for the given priority.  
   * 
   * @param task runnable to execute
   * @param priority priority for task
   */
  public void execute(Runnable task, TaskPriority priority);
  
  /**
   * Submit a task to run as soon as possible for the given priority.  There is a slight increase 
   * in load when using submit over execute.  So this should only be used when the future is 
   * necessary.
   * 
   * The {@link ListenableFuture#get()} method will return {@code null} once the runnable has 
   * completed.
   * 
   * @param task runnable to be executed
   * @param priority priority for task
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority);
  
  /**
   * Submit a task to run as soon as possible for the given priority.  There is a slight increase 
   * in load when using submit over execute.  So this should only be used when the future is 
   * necessary.
   * 
   * The {@link ListenableFuture#get()} method will return the provided result once the runnable has 
   * completed.
   * 
   * @param <T> type of result returned from the future
   * @param task runnable to be executed
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param priority priority for task
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submit(Runnable task, T result, 
                                        TaskPriority priority);

  /**
   * Submit a {@link Callable} to run as soon as possible for the given priority.  This is needed 
   * when a result needs to be consumed from the callable.
   * 
   * @param <T> type of result returned from the future
   * @param task callable to be executed
   * @param priority priority for callable
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority);
  
  /**
   * Schedule a task with a given delay and a specified priority.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @param priority priority to give task for execution
   */
  public void schedule(Runnable task, long delayInMs, TaskPriority priority);
  
  /**
   * Schedule a task with a given delay and a specified priority.  There is a slight increase in 
   * load when using {@link #submitScheduled(Runnable, long, TaskPriority)} over 
   * {@link #schedule(Runnable, long, TaskPriority)}.  So this should only be used when the 
   * future is necessary.
   * 
   * The {@link ListenableFuture#get()} method will return null once the runnable has completed.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @param priority priority to give task for execution
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority);
  
  /**
   * Schedule a task with a given delay and a specified priority.  
   * 
   * The {@link ListenableFuture#get()} method will return the provided result once the runnable 
   * has completed.
   * 
   * @param <T> type of result returned from the future
   * @param task runnable to execute
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @param priority priority to give task for execution
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, 
                                                 TaskPriority priority);
  
  /**
   * Schedule a {@link Callable} with a given delay.  This is needed when a result needs to be 
   * consumed from the callable.
   * 
   * @param <T> type of result returned from the future
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @param priority priority to give task for execution
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority);

  /**
   * Schedule a recurring task to run and a provided priority.  The recurring delay time will be 
   * from the point where execution finished.
   * 
   * @param task runnable to be executed.
   * @param initialDelay delay in milliseconds until first run.
   * @param recurringDelay delay in milliseconds for running task after last finish.
   * @param priority priority for task to run at
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
