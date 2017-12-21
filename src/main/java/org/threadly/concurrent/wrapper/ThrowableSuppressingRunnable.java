package org.threadly.concurrent.wrapper;

import org.threadly.concurrent.RunnableRunnableContainer;
import org.threadly.util.ExceptionUtils;

/**
 * Class that is designed to wrap a runnable, and prevent any throwables from propagating out of 
 * the run function.  If a throwable is thrown, it will be provided to 
 * {@link ExceptionUtils#handleException(Throwable)}.
 * 
 * @since 4.6.0 (since 2.3.0 at org.threadly.concurrent)
 */
public class ThrowableSuppressingRunnable implements RunnableRunnableContainer {
  protected final Runnable task;
  
  /**
   * Constructs a new ThrowableSurpressingRunnable with the provided task.  If the task is 
   * {@code null}, when this is run no operation will occur.
   * 
   * @param task task to be executed and have exceptions prevented from being thrown
   */
  public ThrowableSuppressingRunnable(Runnable task) {
    this.task = task;
  }
  
  @Override
  public void run() {
    if (task != null) {
      ExceptionUtils.runRunnable(task);
    }
  }

  @Override
  public Runnable getContainedRunnable() {
    return task;
  }
}
