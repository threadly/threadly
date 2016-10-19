package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.RejectedExecutionException;

/**
 * Interface to be invoked when a limiter can not accept a task for any reason.  Since in threadly 
 * pools will only reject tasks if the pool is shutdown, this is currently specific to our 
 * limiting wrappers.
 * 
 * @since 4.8.0
 */
public interface RejectedExecutionHandler {
  /**
   * Typically the default handler for most pool implementations.  This handler will only throw a 
   * {@link RejectedExecutionException} to indicate the failure in accepting the task.
   */
  public static final RejectedExecutionHandler THROW_REJECTED_EXECUTION_EXCEPTION = new RejectedExecutionHandler() {
    @Override
    public void handleRejectedTask(Runnable task) {
      throw new RejectedExecutionException("Could not execute task: " + task);
    }
  };
  
  /**
   * Handle the task that was unable to be accepted by a pool.  This function may simply swallow 
   * the task, log, queue in a different way, or throw an exception.  Note that this task may not 
   * be the original task submitted, but an instance of 
   * {@link org.threadly.concurrent.future.ListenableFutureTask} or something similar to convert 
   * Callables or handle other future needs.  In any case the comparison of tasks should be 
   * possible using {@link org.threadly.concurrent.ContainerHelper}.
   * 
   * @param task Task which could not be submitted to the pool
   */
  public void handleRejectedTask(Runnable task);
}
