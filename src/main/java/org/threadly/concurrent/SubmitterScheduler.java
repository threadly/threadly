package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * <p>A thread pool for scheduling tasks with provided futures.  This scheduler submits 
 * runnables/callables and returns futures for when they will be completed.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0 (since 1.0.0 as SubmitterSchedulerInterface_
 */
@SuppressWarnings("deprecation")
public interface SubmitterScheduler extends SimpleSchedulerInterface, 
                                            SubmitterExecutorInterface {
  /**
   * Schedule a task with a given delay.  There is a slight increase in load when using 
   * {@link #submitScheduled(Runnable, long)} over {@link #schedule(Runnable, long)}.  So this 
   * should only be used when the future is necessary.
   * 
   * The {@link ListenableFuture#get()} method will return {@code null} once the runnable has 
   * completed.
   * 
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs);
  
  /**
   * Schedule a task with a given delay.  The {@link ListenableFuture#get()} method will return 
   * the provided result once the runnable has completed.
   * 
   * @param <T> type of result returned from the future
   * @param task runnable to execute
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs);
  
  /**
   * Schedule a {@link Callable} with a given delay.  This is needed when a result needs to be 
   * consumed from the callable.
   * 
   * @param <T> type of result returned from the future
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs);
}
