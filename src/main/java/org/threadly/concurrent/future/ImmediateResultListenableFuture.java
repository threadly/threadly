package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Completed implementation of {@link ListenableFuture} that will immediately return a result.  
 * Meaning listeners added will immediately be ran/executed, {@link FutureCallback}'s will 
 * immediately get called with the result provided, and {@link #get()} calls will never block.
 * 
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
public class ImmediateResultListenableFuture<T> extends AbstractImmediateListenableFuture<T> {
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides a {@code null} 
   * result.  Since this is a common case this can avoid GC overhead.  If you want to get this in 
   * any generic type use {@link FutureUtils#immediateResultFuture(Object)}, which when provided a 
   * {@code null} will always return this static instance.
   * 
   * @since 4.2.0
   */
  public static final ImmediateResultListenableFuture<?> NULL_RESULT = 
      new ImmediateResultListenableFuture<>(null);
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides a {@link Boolean} 
   * in the {@code true} state as the result.
   * 
   * @since 5.6
   */
  public static final ImmediateResultListenableFuture<Boolean> BOOLEAN_TRUE_RESULT = 
      new ImmediateResultListenableFuture<>(Boolean.TRUE);
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides a {@link Boolean} 
   * in the {@code false} state as the result.
   * 
   * @since 5.6
   */
  public static final ImmediateResultListenableFuture<Boolean> BOOLEAN_FALSE_RESULT = 
      new ImmediateResultListenableFuture<>(Boolean.FALSE);
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link String} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<String> EMPTY_STRING_RESULT = 
      new ImmediateResultListenableFuture<>("");
  
  protected final T result;
  
  /**
   * Constructs a completed future that will return the provided result.
   * 
   * @param result Result that is returned by future
   */
  public ImmediateResultListenableFuture(T result) {
    this.result = result;
  }
  
  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper, 
                                                               Executor executor) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                               Function<? super TT, ? extends T> mapper, 
                                                               Executor executor, 
                                                               ListenerOptimizationStrategy optimizeExecution) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper, 
                                                                   Executor executor) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                   Function<? super TT, ListenableFuture<T>> mapper, 
                                                                   Executor executor, 
                                                                   ListenerOptimizationStrategy optimizeExecution) {
    return this;  // nothing to map, we are not in error
  }

  @Override
  public ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor, 
                                      ListenerOptimizationStrategy optimize) {
    if (executor == null | 
        optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      callback.handleResult(result);
    } else {
      executor.execute(() -> callback.handleResult(result));
    }
    
    return this;
  }

  @Override
  public ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor, 
                                            ListenerOptimizationStrategy optimize) {
    if (executor == null | 
        optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      callback.accept(result);
    } else {
      executor.execute(() -> callback.accept(result));
    }
    
    return this;
  }

  @Override
  public ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor, 
                                             ListenerOptimizationStrategy optimize) {
    // ignored
    return this;
  }
  
  @Override
  public T get() {
    return result;
  }

  @Override
  public T get(long timeout, TimeUnit unit) {
    return result;
  }
}