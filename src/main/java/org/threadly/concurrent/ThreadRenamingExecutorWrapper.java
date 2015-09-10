package org.threadly.concurrent;

import java.util.concurrent.Executor;

/**
 * <p>Class which wraps a {@link Executor} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnableWrapper}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.</p>
 * 
 * @author jent
 * @since 4.3.0
 */
public class ThreadRenamingExecutorWrapper extends AbstractSubmitterExecutor {
  protected final Executor executor;
  protected final String threadName;
  protected final boolean replace;
  
  /**
   * Constructs a new {@link ThreadRenamingExecutorWrapper}, wrapping a supplied {@link Executor}.  If 
   * {@code replace} is {@code false} the thread will be named such that 
   * {@code threadName[originalThreadName]}.
   * 
   * @param executor Executor to wrap and send executions to
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingExecutorWrapper(Executor executor, String threadName, boolean replace) {
    this.executor = executor;
    this.threadName = threadName;
    this.replace = replace;
  }

  @Override
  protected void doExecute(Runnable task) {
    executor.execute(new ThreadRenamingRunnableWrapper(task, threadName, replace));
  }
}