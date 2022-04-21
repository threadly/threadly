package org.threadly.concurrent.processing;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * Implementation of {@link FlowControlledProcessor} which reduces boiler plate code when no result 
 * is expected.  Instead all logic should be done in the future returned by {@link #next()}.
 * <p>
 * To further minimize code this extends {@link FlowControlledNoFailureProcessor} where by default 
 * all exceptions are considered failures.  If you want to handle some failures you can override 
 * {@link #handleFailure(Throwable)} returning {@code true} to indicate an exception as expected.
 *
 * @since 5.37
 */
public abstract class FlowControlledNoResultProcessor extends FlowControlledNoFailureProcessor<Object> {
  /**
   * Construct a new processor.  You must invoke {@link #start()} once constructed to start 
   * processing.
   * 
   * @param maxRunningTasks Maximum number of concurrent running tasks
   */
  public FlowControlledNoResultProcessor(int maxRunningTasks) {
    super(maxRunningTasks, false);
  }

  @Override
  protected abstract ListenableFuture<?> next();

  @Override
  protected final void handleResult(Object result) {
    // ignored
  }
}
