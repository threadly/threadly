package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * A thread pool for executing tasks with provided futures.  This executor submits 
 * runnables/callables and returns futures for when they will be completed.
 * 
 * @since 4.3.0 (since 1.0.0 as SubmitterExecutorInterface)
 */
public interface SubmitterExecutor extends Executor {
  /**
   * Submit a task to run as soon as possible.  There is a slight increase in load when using 
   * {@link #submit(Runnable)} over {@link #execute(Runnable)}.  So this should only be used when 
   * the returned future is necessary.  
   * <p>
   * The {@link ListenableFuture#get()} method will return {@code null} once the runnable has 
   * completed.
   * 
   * @param task runnable to be executed
   * @return a future to know when the task has completed
   */
  default ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }
  
  /**
   * Submit a task to run as soon as possible.  The {@link ListenableFuture#get()} method will 
   * return the provided result once the runnable has completed.
   * 
   * @param <T> type of result for future
   * @param task runnable to be executed
   * @param result result to be returned from resulting future .get() when runnable completes
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submit(Runnable task, T result);

  /**
   * Submit a {@link Callable} to run as soon as possible.  This is needed when a result needs to 
   * be consumed from the callable.
   * 
   * @param <T> type of result returned from the future
   * @param task callable to be executed
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submit(Callable<T> task);
}
