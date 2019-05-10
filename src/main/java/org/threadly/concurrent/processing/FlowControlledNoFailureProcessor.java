package org.threadly.concurrent.processing;

/**
 * Implementation of {@link FlowControlledProcessor} which assumes all exceptions are unexpected.  
 * Since this is a common condition this class can help reduce some boiler plate code.
 *
 * @since 5.37
 * @param <T> The type of result produced / accepted
 */
public abstract class FlowControlledNoFailureProcessor<T> extends FlowControlledProcessor<T> {
  /**
   * Construct a new processor.  You must invoke {@link #start()} once constructed to start 
   * processing.
   * 
   * @param maxRunningTasks Maximum number of concurrent running tasks
   * @param provideResultsInOrder If {@code true} completed results will be provided in the order they are submitted
   */
  public FlowControlledNoFailureProcessor(int maxRunningTasks, boolean provideResultsInOrder) {
    super(maxRunningTasks, provideResultsInOrder);
  }

  @Override
  protected boolean handleFailure(Throwable t) {
    return false;
  }
}
