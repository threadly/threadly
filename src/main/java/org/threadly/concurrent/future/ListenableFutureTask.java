package org.threadly.concurrent.future;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.function.Function;

import org.threadly.concurrent.CallableContainer;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.event.RunnableListenerHelper;

/**
 * <p>This is a future which can be executed.  Allowing you to construct the future with the 
 * interior work, submit it to an {@link Executor}, and then return this future.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public class ListenableFutureTask<T> extends FutureTask<T> 
                                     implements ListenableRunnableFuture<T>, 
                                                CallableContainer<T> {
  protected final RunnableListenerHelper listenerHelper;
  protected final boolean recurring;
  protected final Callable<T> callable;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   */
  public ListenableFutureTask(boolean recurring, Runnable task) {
    this(recurring, task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result) {
    this(recurring, new RunnableCallableAdapter<T>(task, result));
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task) {
    super(task);

    this.listenerHelper = new RunnableListenerHelper(true);
    this.recurring = recurring;
    this.callable = task;
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
  public void addListener(Runnable listener) {
    listenerHelper.addListener(listener);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    listenerHelper.addListener(listener, executor);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    addCallback(callback, null);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    addListener(new RunnableFutureCallbackAdapter<T>(this, callback), executor);
  }

  @Override
  public <R> ListenableFuture<R> map(Function<? super T, R> mapper) {
    return map(mapper, null);
  }

  @Override
  public <R> ListenableFuture<R> map(Function<? super T, R> mapper, Executor executor) {
    return FutureUtils.transform(this, mapper, executor);
  }
  
  /**
   * Can not be overridden, please use {@link #addListener(Runnable)} as an alternative.
   */
  @Override
  protected final void done() {
    listenerHelper.callListeners();
  }

  @Override
  public Callable<T> getContainedCallable() {
    return callable;
  }
}