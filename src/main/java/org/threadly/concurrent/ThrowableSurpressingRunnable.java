package org.threadly.concurrent;

/**
 * <p>Class that is designed to wrap a runnable, and prevent any 
 * throwables from proegating out of the run function.  If a throwable 
 * is thrown, it will be provided to 
 * {@link org.threadly.util.ExceptionUtils}.handleException(Throwable).</p>
 * 
 * @deprecated Use ThrowableSuppressingRunnable
 * 
 * @author jent - Mike Jensen
 * @since 2.1.0
 */
@Deprecated
public class ThrowableSurpressingRunnable extends ThrowableSuppressingRunnable {
  /**
   * Constructs a new ThrowableSurpressingRunnable with the provided task.  
   * If the task is null, when this is run no operation will occur.
   * 
   * @param task task to be executed and have exceptions prevented from being thrown
   */
  public ThrowableSurpressingRunnable(Runnable task) {
    super(task);
  }
}
