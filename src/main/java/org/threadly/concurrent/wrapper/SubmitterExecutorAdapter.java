package org.threadly.concurrent.wrapper;

import java.util.concurrent.Executor;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.util.ArgumentVerifier;

/**
 * A simple wrapper class for {@link Executor} implementations to provide 
 * {@link org.threadly.concurrent.SubmitterExecutor} capabilities.
 * <p>
 * In addition this implementation returns {@link org.threadly.concurrent.future.ListenableFuture} 
 * future implementations, making it an easy way to convert your favorite executor to use 
 * ListenableFutures.
 * 
 * @since 4.8.0 (since 1.0.0 as org.threadly.concurrent.ExecutorWrapper)
 */
public class SubmitterExecutorAdapter extends AbstractSubmitterExecutor {
  protected final Executor executor;
  
  /**
   * Constructors a new wrapper instance with the provided executor to defer calls to.
   * 
   * @param executor {@link Executor} instance.
   */
  public SubmitterExecutorAdapter(Executor executor) {
    ArgumentVerifier.assertNotNull(executor, "executor");
    
    this.executor = executor;
  }

  @Override
  protected void doExecute(Runnable task) {
    executor.execute(task);
  }
}
