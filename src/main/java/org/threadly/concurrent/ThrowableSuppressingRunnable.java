package org.threadly.concurrent;

/**
 * <p>Class that is designed to wrap a runnable, and prevent any throwables from propagating out 
 * of the run function.  If a throwable is thrown, it will be provided to 
 * {@link org.threadly.util.ExceptionUtils}.handleException(Throwable).</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.ThrowableSuppressingRunnable}
 * 
 * @author jent - Mike Jensen
 * @since 2.3.0
 */
@Deprecated
public class ThrowableSuppressingRunnable extends org.threadly.concurrent.wrapper.ThrowableSuppressingRunnable 
                                          implements RunnableContainerInterface {
  /**
   * Constructs a new ThrowableSurpressingRunnable with the provided task.  If the task is 
   * {@code null}, when this is run no operation will occur.
   * 
   * @param task task to be executed and have exceptions prevented from being thrown
   */
  public ThrowableSuppressingRunnable(Runnable task) {
    super(task);
  }
}
