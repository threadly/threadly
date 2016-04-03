package org.threadly.concurrent;

/**
 * <p>A simple runnable wrapper which will rename the thread during execution, and set the name 
 * back at the end of execution.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.traceability.ThreadRenamingRunnableWrapper}
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
@Deprecated
public class ThreadRenamingRunnableWrapper 
                 extends org.threadly.concurrent.wrapper.traceability.ThreadRenamingRunnableWrapper {
  /**
   * Constructs a new {@link ThreadRenamingRunnableWrapper}.  If {@code replace} is {@code false} 
   * the thread will be named such that {@code threadName[originalThreadName]}.
   * 
   * @param runnable Runnable which should be executed
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingRunnableWrapper(Runnable runnable, String threadName, boolean replace) {
    super(runnable, threadName, replace);
  }
}