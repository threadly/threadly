package org.threadly.concurrent.future;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.threadly.concurrent.CallableContainer;
import org.threadly.concurrent.RunnableCallableAdapter;

/**
 * This is a future which can be executed.  Allowing you to construct the future with the interior 
 * work, submit it to an {@link Executor}, and then return this future.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public class ListenableFutureTask<T> extends AbstractCompletableListenableFuture<T>
                                     implements ListenableRunnableFuture<T>, 
                                                CallableContainer<T> {
  private static final VarHandle EXEC_THREAD;
  
  static {
    try {
      MethodHandles.Lookup l = MethodHandles.lookup();
      EXEC_THREAD = l.findVarHandle(AbstractCompletableListenableFuture.class, "execThread", Thread.class);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }
  
  protected Callable<T> callable;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   */
  public ListenableFutureTask(Runnable task) {
    this(task, null, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ListenableFutureTask(Runnable task, T result) {
    this(task, result, null);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   */
  public ListenableFutureTask(Callable<T> task) {
    this(task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(Runnable task, T result, Executor executingExecutor) {
    this(RunnableCallableAdapter.adapt(task, result), executingExecutor);
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   * @param executingExecutor Executor task will be run on for possible listener optimization, or {@code null}
   */
  public ListenableFutureTask(Callable<T> task, Executor executingExecutor) {
    super(executingExecutor);
    
    this.callable = task;
  }
  
  @Override
  protected void handleCompleteState() {
    callable = null;
  }
  
  @Override
  public void run() {
    if (state != STATE_NEW || ! EXEC_THREAD.compareAndSet(this, null, Thread.currentThread())) {
      return;
    }
    try {
      Callable<T> c = callable;
      if (c != null && state == STATE_NEW) {
        completeWithResult(c.call());
      }
    } catch (Throwable t) {
      completeWithFailure(t);
    } finally {
      // make sure we contain interrupts before we release the thread
      while (state == STATE_INTERRUPTING) {
        Thread.yield();
      }
      // we must unset execThread last to protect against concurrent execution
      execThread = null;
    }
  }

  @Override
  public Callable<T> getContainedCallable() {
    return callable;
  }
  
  @Override
  public String toString() {
    return super.toString(callable);
  }
}
