package org.threadly.concurrent;

import org.threadly.util.ExceptionUtils;

/**
 * <p>Class that is designed to wrap a runnable, and prevent any 
 * throwables from proegating out of the run function.  If a throwable 
 * is thrown, it will be provided to 
 * {@link ExceptionUtils}.handleException(Throwable).</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.3.0
 */
public class ThrowableSuppressingRunnable implements RunnableContainerInterface, Runnable {
  private final Runnable task;
  
  /**
   * Constructs a new ThrowableSurpressingRunnable with the provided task.  
   * If the task is null, when this is run no operation will occur.
   * 
   * @param task task to be executed and have exceptions prevented from being thrown
   */
  public ThrowableSuppressingRunnable(Runnable task) {
    this.task = task;
  }
  
  @Override
  public void run() {
    try {
      if (task != null) {
        task.run();
      }
    } catch (Throwable t) {
      ExceptionUtils.handleException(t);
    }
  }

  @Override
  public Runnable getContainedRunnable() {
    return task;
  }
}
