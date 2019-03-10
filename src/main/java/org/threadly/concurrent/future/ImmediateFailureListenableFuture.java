package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Completed implementation of {@link ListenableFuture} that will immediately provide a failure 
 * condition.  Meaning listeners added will immediately be ran/executed, {@link FutureCallback}'s 
 * will immediately get called with the throwable provided, and {@link #get()} will immediately 
 * throw an {@link ExecutionException}.
 * 
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
public class ImmediateFailureListenableFuture<T> extends AbstractImmediateListenableFuture<T> {
  protected final Throwable failure;
  
  /**
   * Constructs a completed future with the provided failure.  If the failure is {@code null}, a 
   * new {@link Exception} will be created to represent it.
   * 
   * @param failure to be the cause of the ExecutionException from {@link #get()} calls
   */
  public ImmediateFailureListenableFuture(Throwable failure) {
    if (failure != null) {
      this.failure = failure;
    } else {
      this.failure = new Exception();
    }
  }

  @Override
  public ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    if (executor == null | 
        optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      callback.handleFailure(failure);
    } else {
      executor.execute(() -> callback.handleFailure(failure));
    }
    
    return this;
  }

  @Override
  public ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor, 
                                            ListenerOptimizationStrategy optimize) {
    // ignored
    return this;
  }

  @Override
  public ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor, 
                                             ListenerOptimizationStrategy optimize) {
    if (executor == null | 
        optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      callback.accept(failure);
    } else {
      executor.execute(() -> callback.accept(failure));
    }
    
    return this;
  }

  @Override
  public T get() throws ExecutionException {
    throw new ExecutionException(failure);
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws ExecutionException {
    throw new ExecutionException(failure);
  }
}