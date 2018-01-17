package org.threadly.concurrent.future;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import org.threadly.concurrent.CallableContainer;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.event.RunnableListenerHelper;

/**
 * This is a future which can be executed.  Allowing you to construct the future with the interior 
 * work, submit it to an {@link Executor}, and then return this future.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public class ListenableFutureTask<T> extends FutureTask<T> 
                                     implements ListenableRunnableFuture<T>, 
                                                CallableContainer<T> {
  protected final RunnableListenerHelper listenerHelper;
  protected final boolean recurring;
  protected Callable<T> callable;
  private Executor executingExecutor;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   */
  public ListenableFutureTask(boolean recurring, Runnable task) {
    this(recurring, task, null, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result) {
    this(recurring, task, result, null);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task) {
    this(recurring, task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(boolean recurring, Runnable task, Executor executingExecutor) {
    this(recurring, task, null, executingExecutor);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result, Executor executingExecutor) {
    this(recurring, new RunnableCallableAdapter<>(task, result), executingExecutor);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task, Executor executingExecutor) {
    super(task);

    this.listenerHelper = new RunnableListenerHelper(true);
    this.recurring = recurring;
    this.callable = task;
    this.executingExecutor = executingExecutor;
  }
  
  @Override
  public void run() {
    if (recurring) {
      super.runAndReset();
    } else {
      super.run();
    }
  }

  @Override
  public void addListener(Runnable listener, Executor executor, 
                          ListenerOptimizationStrategy optimize) {
    if (isDone()) {
      // if we add the below condition above, we must check `done` again for the second check
      if (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
        executor = null;
      }
    } else if (executor == executingExecutor) {
      if (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone || 
          optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) {
        executor = null;
      }
    }
    listenerHelper.addListener(listener, executor);
  }
  
  /**
   * Can not be overridden, please use {@link #addListener(Runnable)} as an alternative.
   */
  @Override
  protected final void done() {
    executingExecutor = null;
    callable = null;
    
    listenerHelper.callListeners();
  }

  @Override
  public Callable<T> getContainedCallable() {
    return callable;
  }
}