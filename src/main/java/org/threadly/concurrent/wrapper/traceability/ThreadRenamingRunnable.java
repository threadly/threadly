package org.threadly.concurrent.wrapper.traceability;

import org.threadly.concurrent.RunnableContainer;
import org.threadly.util.ArgumentVerifier;

/**
 * A simple runnable wrapper which will rename the thread during execution, and set the name back 
 * at the end of execution.
 * 
 * @since 4.8.0 (since 4.3.0 as org.threadly.concurrent.ThreadRenamingRunnableWrapper)
 */
public class ThreadRenamingRunnable implements Runnable, RunnableContainer {
  protected final Runnable runnable;
  protected final String threadName;
  protected final boolean replace;
  
  /**
   * Constructs a new {@link ThreadRenamingRunnable}.  If {@code replace} is {@code false} 
   * the thread will be named such that {@code threadName[originalThreadName]}.
   * 
   * @param runnable Runnable which should be executed
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingRunnable(Runnable runnable, String threadName, boolean replace) {
    ArgumentVerifier.assertNotNull(runnable, "runnable");
    
    this.runnable = runnable;
    this.threadName = threadName;
    this.replace = replace;
  }

  @Override
  public void run() {
    Thread t = Thread.currentThread();
    String originalName = t.getName();
    try {
      if (replace) {
        t.setName(threadName);
      } else {
        t.setName(threadName + '[' + originalName + ']');
      }
      
      runnable.run();
    } finally {
      t.setName(originalName);
    }
  }

  @Override
  public Runnable getContainedRunnable() {
    return runnable;
  }
}