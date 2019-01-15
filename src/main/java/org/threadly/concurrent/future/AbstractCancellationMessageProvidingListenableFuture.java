package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Class used to represent when a {@link ListenableFuture} supports setting a custom message in the 
 * {@link CancellationException}.  This should be done by overriding 
 * {@link #getCancellationExceptionMessage()}.
 *
 * @param <T> The result object type returned by this future
 */
abstract class AbstractCancellationMessageProvidingListenableFuture<T> implements ListenableFuture<T> {
  /**
   * Invoked when a {@link CancellationException} is constructed.  By default this will return 
   * {@code null}, but it may be overridden if a custom message wants to be included with the 
   * {@link CancellationException}.  This can be useful for extending the class and recording 
   * state when a cancel occurs, then later providing this state as a message in the exception.
   * 
   * @return The message to be provided to the {@link CancellationException} constructor, or {@code null}
   */
  protected String getCancellationExceptionMessage() {
    return null;  // by default null is provided
  }
  
  @Override
  public <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, true, null, null);
  }
  
  @Override
  public <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper, Executor executor) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, true, executor, null);
  }
  
  @Override
  public <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper, Executor executor, 
                                     ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, true, executor, optimizeExecution);
  }
  
  @Override
  public <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, false, null, null);
  }
  
  @Override
  public <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper, 
                                          Executor executor) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, false, executor, null);
  }
  
  @Override
  public <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper, Executor executor, 
                                          ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.transform(this, this::getCancellationExceptionMessage, 
                                         mapper, false, executor, optimizeExecution);
  }
  
  @Override
  public <R> ListenableFuture<R> flatMap(ListenableFuture<R> future) {
    return InternalFutureUtils.flatTransform(this, this::getCancellationExceptionMessage, 
                                             (ignored) -> future, null, null);
  }
  
  @Override
  public <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper) {
    return InternalFutureUtils.flatTransform(this, this::getCancellationExceptionMessage, 
                                             mapper, null, null);
  }

  @Override
  public <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper, 
                                         Executor executor) {
    return InternalFutureUtils.flatTransform(this, this::getCancellationExceptionMessage, 
                                             mapper, executor, null);
  }

  @Override
  public <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper, 
                                         Executor executor, 
                                         ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.flatTransform(this, this::getCancellationExceptionMessage, 
                                             mapper, executor, optimizeExecution);
  }
  
  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper) {
    return InternalFutureUtils.failureTransform(this, this::getCancellationExceptionMessage, 
                                                mapper, throwableType, null, null);
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper, 
                                                               Executor executor) {
    return InternalFutureUtils.failureTransform(this, this::getCancellationExceptionMessage, 
                                                mapper, throwableType, executor, null);
  }
  
  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper, 
                                                               Executor executor, 
                                                               ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.failureTransform(this, this::getCancellationExceptionMessage, mapper, 
                                                throwableType, executor, optimizeExecution);
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper) {
    return InternalFutureUtils.flatFailureTransform(this, this::getCancellationExceptionMessage, 
                                                    mapper, throwableType, null, null);
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper, 
                                                                   Executor executor) {
    return InternalFutureUtils.flatFailureTransform(this, this::getCancellationExceptionMessage, 
                                                    mapper, throwableType, executor, null);
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper, 
                                                                   Executor executor, 
                                                                   ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.flatFailureTransform(this, this::getCancellationExceptionMessage, 
                                                    mapper, throwableType, 
                                                    executor, optimizeExecution);
  }
}
