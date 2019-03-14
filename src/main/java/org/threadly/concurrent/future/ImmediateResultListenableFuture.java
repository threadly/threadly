package org.threadly.concurrent.future;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
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
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link Optional} from {@link Optional#empty()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends Optional<?>> EMPTY_OPTIONAL_RESULT = 
      new ImmediateResultListenableFuture<>(Optional.empty());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link List} from {@link Collections#emptyList()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends List<?>> EMPTY_LIST_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptyList());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link Map} from {@link Collections#emptyMap()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends Map<?, ?>> EMPTY_MAP_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptyMap());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link SortedMap} from {@link Collections#emptyMap()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends SortedMap<?, ?>> EMPTY_SORTED_MAP_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptySortedMap());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link Set} from {@link Collections#emptySet()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends Set<?>> EMPTY_SET_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptySet());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link SortedSet} from {@link Collections#emptySet()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends SortedSet<?>> EMPTY_SORTED_SET_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptySortedSet());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link Iterator} from {@link Collections#emptyIterator()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends Iterator<?>> EMPTY_ITERATOR_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptyIterator());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link ListIterator} from {@link Collections#emptyListIterator()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends ListIterator<?>> EMPTY_LIST_ITERATOR_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptyListIterator());
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides an empty 
   * {@link Enumeration} from {@link Collections#emptyEnumeration()} as the result.
   * 
   * @since 5.34
   */
  public static final ImmediateResultListenableFuture<? extends Enumeration<?>> EMPTY_ENUMERATION_RESULT = 
      new ImmediateResultListenableFuture<>(Collections.emptyEnumeration());
  
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