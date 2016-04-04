package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * <p>A simple wrapper class for {@link Executor} implementations to provide 
 * {@link SubmitterExecutor} capabilities.</p>
 * 
 * <p>In addition this implementation returns 
 * {@link org.threadly.concurrent.future.ListenableFuture} future implementations, making it an 
 * easy way to convert your favorite executor to use ListenableFutures.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.ExecutorWrapper}
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class ExecutorWrapper extends org.threadly.concurrent.wrapper.ExecutorWrapper {
  /**
   * Constructors a new wrapper instance with the provided executor to defer calls to.
   * 
   * @param executor {@link Executor} instance.
   */
  public ExecutorWrapper(Executor executor) {
    super(executor);
  }
}
