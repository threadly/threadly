package org.threadly.concurrent;

import java.util.concurrent.Executor;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>A simple wrapper class for {@link Executor} implementations to 
 * provide {@link SubmitterExecutorInterface} capabilities.</p>
 * 
 * <p>In addition this implementation returns 
 * {@link org.threadly.concurrent.future.ListenableFuture} 
 * future implementations, making it an easy way to convert your favorite 
 * executor to use ListenableFutures.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ExecutorWrapper extends AbstractSubmitterExecutor {
  protected final Executor executor;
  
  /**
   * Constructors a new wrapper instance with the provided 
   * executor to defer calls to.
   * 
   * @param executor {@link Executor} instance.
   */
  public ExecutorWrapper(Executor executor) {
    ArgumentVerifier.assertNotNull(executor, "executor");
    
    this.executor = executor;
  }

  @Override
  protected void doExecute(Runnable task) {
    executor.execute(task);
  }
}
