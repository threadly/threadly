package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Future where you can add a listener which is called once the future has completed.  The 
 * runnable will be called once the future completes either as a cancel, with result, or with an 
 * exception.
 * 
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public interface ListenableFuture<T> extends Future<T> {
  /**
   * Strategy to use for optimizing listener execution.  By default no optimization will take place, 
   * but in certain use cases this optimization can significantly improve performance.  For 
   * listeners which have fast / simple work loads, but which require a specific executor for 
   * thread safety, this can provide hints when the listener can be executed either in the calling 
   * thread or in a single threaded manner.  By allowing these types of executions the listener 
   * does not need to re-queue on the executor.  Allowing it to both skip the queue if the 
   * executor is backed up, but also reducing the amount of cross thread communication.
   * 
   * @since 5.10
   */
  public enum ListenerOptimizationStrategy {
    /**
     * The default strategy, no optimization is assumed.  This is safest because listeners may 
     * block or otherwise require the executor to be provided to always be respected.
     */
    None, 
    /**
     * This will optimize away the executor if the executor provided is the same one that the task 
     * WILL complete on.  If the task is already completed then it is assumed that execution can 
     * NOT occur on the calling thread.  If the calling thread can allow execution please see 
     * {@link #SingleThreadIfExecutorMatchOrDone}.
     */
    SingleThreadIfExecutorMatch,
    /**
     * Similar to {@link #SingleThreadIfExecutorMatch} this will optimize away the executor if 
     * it will complete on the same executor provided.  But this also adds the optimization that if 
     * the future is complete, it is assumed that it can now execute in the calling thread.  This 
     * is typically useful if you are adding a callback on a thread that is also executing on the 
     * executor you are providing.
     */
    SingleThreadIfExecutorMatchOrDone
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper and 
   * {@link org.threadly.util.ExceptionUtils#handleException(Throwable)} will be invoked.  If you 
   * don't want mapped exceptions to be treated as unexpected / uncaught please see 
   * {@link #throwMap(Function)}.
   * <p>
   * This can be easily used to chain together a series of operations, happening async (or in 
   * calling thread if already complete) until the final result is actually needed.  
   * {@link #map(Function, Executor)} can be used if transformation mapper is expensive and thus 
   * async execution is absolutely required.  
   * <p>
   * If the future is complete already, the function may be invoked on the invoking thread.  If the 
   * future is not complete then the function will be invoked on the thread which completes 
   * the future (immediately after it completes).  
   * <p>
   * If your function returns a future, consider using {@link #flatMap(Function)} as an alternative.  
   * <p>
   * Example use:
   * <pre>{@code 
   *   public Integer countSomething(String value);
   *   
   *   public ListenableFuture<String> lookupSomething();
   *   
   *   ListenableFuture<Integer> count = lookupSomething().map((s) -> countSomething(s));
   * }</pre>
   * 
   * @since 5.0
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper) {
    return map(mapper, null, null);
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper and 
   * {@link org.threadly.util.ExceptionUtils#handleException(Throwable)} will be invoked.  If you 
   * don't want mapped exceptions to be treated as unexpected / uncaught please see 
   * {@link #throwMap(Function, Executor)}.
   * <p>
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.  If the mapper function is very fast and cheap to run then {@link #map(Function)} or 
   * providing {@code null} for the executor can allow more efficient operation.  
   * <p>
   * If your function returns a future, consider using {@link #flatMap(Function, Executor)} as an 
   * alternative.
   * 
   * @since 5.0
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper, Executor executor) {
    return map(mapper, executor, null);
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper and 
   * {@link org.threadly.util.ExceptionUtils#handleException(Throwable)} will be invoked.  If you 
   * don't want mapped exceptions to be treated as unexpected / uncaught please see 
   * {@link #throwMap(Function, Executor, ListenerOptimizationStrategy)}.
   * <p>
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.  If the mapper function is very fast and cheap to run then {@link #map(Function)} or 
   * providing {@code null} for the executor can allow more efficient operation.  
   * <p>
   * If your function returns a future, consider using {@link #flatMap(Function, Executor)} as an 
   * alternative.
   * <p>
   * Caution should be used when choosing to optimize the listener execution.  If the listener is 
   * complex, or wanting to be run concurrent, this optimization could prevent that.  In addition 
   * it will prevent other listeners from potentially being invoked until it completes.  However 
   * if the listener is small / fast, this can provide significant performance gains.  It should 
   * also be known that not all {@link ListenableFuture} implementations may be able to do such an 
   * optimization.  Please see {@link ListenerOptimizationStrategy} javadocs for more specific 
   * details of what optimizations are available.
   * 
   * @since 5.10
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> map(Function<? super T, ? extends R> mapper, Executor executor, 
                                      ListenerOptimizationStrategy optimizeExecution) {
    return FutureUtils.transform(this, mapper, true, executor, optimizeExecution);
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.
   * <p>
   * This function differs from {@link #map(Function)} only in how exceptions thrown from the 
   * {@code mapper} function are handled.  Please see {@link #map(Function)} for a general 
   * understanding for the behavior of this operation.  This is to be used when the mapper function 
   * throwing an exception is EXPECTED.  Used to avoid treating mapping an exception as an 
   * unexpected behavior.
   * <p>
   * Say for example you wanted to use {@code FutureUtils.scheduleWhile()} to retry an operation 
   * as long as it kept throwing an exception.  You might put the result and exception into 
   * a {@code Pair} so it can be checked for a maximum number of times like this:
   * <pre>{@code 
   *   ListenableFuture<Pair<ResultType, RetryException>> exceptionConvertingLoop =
   *        FutureUtils.scheduleWhile(scheduler, retryDelayMillis, true, () -> {
   *          try {
   *            return new Pair<>(makeResultOrThrow(), null);
   *          } catch (RetryException e) {
   *            // we can't throw or we will break the loop
   *           return new Pair<>(null, e);
   *          }
   *        }, new Predicate<Pair<?, RetryException>>() {
   *          private int selfRetryCount = 0;
   *
   *          public boolean test(Pair<?, RetryException> p) {
   *            return p.getLeft() == null && ++selfRetryCount <= maxRetryCount;
   *          }
   *        });
   * }</pre> 
   * You would then need to use {@link #throwMap(Function)} in order to convert that {@code Pair}
   * back into a {@link ListenableFuture} with either a result, or the contained failure.  For 
   * example:
   * <pre>{@code 
   * ListenableFuture<ResultType> resultFuture =
   *     exceptionConvertingLoop.throwMap((p) -> {
   *       if (p.getLeft() != null) {
   *         return p.getLeft();
   *       } else {
   *         throw p.getRight();
   *       }
   *     });
   * }</pre>
   * 
   * @since 5.11
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper) {
    return throwMap(mapper, null, null);
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.
   * <p>
   * This function differs from {@link #map(Function, Executor)} only in how exceptions thrown 
   * from the {@code mapper} function are handled.  Please see {@link #map(Function, Executor)} 
   * for a general understanding for the behavior of this operation.  This is to be used when the 
   * mapper function throwing an exception is EXPECTED.  Used to avoid treating mapping an 
   * exception as an unexpected behavior.  Please see {@link #throwMap(Function)} for more usage 
   * examples.
   * <p>
   * Once the future completes the mapper function will be invoked on the executor (if provided).  
   * Because of that providing an executor can ensure this will never block.  If an executor is not 
   * provided then the mapper may be invoked on the calling thread (if the future is already 
   * complete), or on the same thread which the future completes on.  If the mapper function is 
   * very fast and cheap to run then {@link #throwMap(Function)} or providing {@code null} for 
   * the executor can allow more efficient operation.  
   * 
   * @since 5.11
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper, 
                                           Executor executor) {
    return throwMap(mapper, executor, null);
  }
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper.  
   * <p>
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.  If the mapper function is very fast and cheap to run then {@link #throwMap(Function)} or 
   * providing {@code null} for the executor can allow more efficient operation.  
   * <p>
   * Caution should be used when choosing to optimize the listener execution.  If the listener is 
   * complex, or wanting to be run concurrent, this optimization could prevent that.  In addition 
   * it will prevent other listeners from potentially being invoked until it completes.  However 
   * if the listener is small / fast, this can provide significant performance gains.  It should 
   * also be known that not all {@link ListenableFuture} implementations may be able to do such an 
   * optimization.  Please see {@link ListenerOptimizationStrategy} javadocs for more specific 
   * details of what optimizations are available.
   * 
   * @since 5.11
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> throwMap(Function<? super T, ? extends R> mapper, Executor executor, 
                                           ListenerOptimizationStrategy optimizeExecution) {
    return FutureUtils.transform(this, mapper, false, executor, optimizeExecution);
  }
  
  /**
   * Similar to {@link #map(Function)}, in that this will apply a mapper function once the applied 
   * to future completes.  Once this future resolves it will provide the result into the provided 
   * function.  Unlike {@link #map(Function)}, this will then unwrap a future provided from the 
   * function so that instead of having {@code ListenableFuture<ListenableFuture<R>>} you can 
   * simply extract the final value.  The returned future will only resolve once the future of the 
   * provided function completes.  
   * <p>
   * If the future is complete already, the function may be invoked on the invoking thread.  If the 
   * future is not complete then the function will be invoked on the thread which completes 
   * the future (immediately after it completes).    
   * <p>
   * Example use:
   * <pre>{@code 
   *   public ListenableFuture<Integer> countSomething(String value);
   *   
   *   public ListenableFuture<String> lookupSomething();
   *   
   *   ListenableFuture<Integer> count = lookupSomething().flatMap((s) -> countSomething(s));
   * }</pre>
   * 
   * @since 5.0
   * @param <R> The type for the object contained in the future which is returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper) {
    return flatMap(mapper, null, null);
  }

  /**
   * Similar to {@link #map(Function, Executor)}, in that this will apply a mapper function once 
   * the applied to future completes.  Once this future resolves it will provide the result into 
   * the provided function.  Unlike {@link #map(Function, Executor)}, this will then unwrap a 
   * future provided from the function so that instead of having 
   * {@code ListenableFuture<ListenableFuture<R>>} you can simply extract the final value.  The 
   * returned future will only resolve once the future of the provided function completes.  
   * <p>
   * Once the future completes the mapper function will be invoked on the executor (if provided).  
   * Because of that providing an executor can ensure this will never block.  If an executor is 
   * not provided then the mapper may be invoked on the calling thread (if the future is already 
   * complete), or on the same thread which the future completes on.  If the mapper function is 
   * very fast and cheap to run then {@link #flatMap(Function)} or providing {@code null} for the 
   * executor can allow more efficient operation.  
   * 
   * @since 5.0
   * @param <R> The type for the object contained in the future which is returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper, 
                                          Executor executor) {
    return flatMap(mapper, executor, null);
  }

  /**
   * Similar to {@link #map(Function, Executor)}, in that this will apply a mapper function once 
   * the applied to future completes.  Once this future resolves it will provide the result into 
   * the provided function.  Unlike {@link #map(Function, Executor)}, this will then unwrap a 
   * future provided from the function so that instead of having 
   * {@code ListenableFuture<ListenableFuture<R>>} you can simply extract the final value.  The 
   * returned future will only resolve once the future of the provided function completes.  
   * <p>
   * Once the future completes the mapper function will be invoked on the executor (if provided).  
   * Because of that providing an executor can ensure this will never block.  If an executor is 
   * not provided then the mapper may be invoked on the calling thread (if the future is already 
   * complete), or on the same thread which the future completes on.  If the mapper function is 
   * very fast and cheap to run then {@link #flatMap(Function)} or providing {@code null} for the 
   * executor can allow more efficient operation.  
   * <p>
   * Caution should be used when choosing to optimize the listener execution.  If the listener is 
   * complex, or wanting to be run concurrent, this optimization could prevent that.  In addition 
   * it will prevent other listeners from potentially being invoked until it completes.  However 
   * if the listener is small / fast, this can provide significant performance gains.  It should 
   * also be known that not all {@link ListenableFuture} implementations may be able to do such an 
   * optimization.  Please see {@link ListenerOptimizationStrategy} javadocs for more specific 
   * details of what optimizations are available.
   * 
   * @since 5.10
   * @param <R> The type for the object contained in the future which is returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return A new {@link ListenableFuture} with the specified result type
   */
  default <R> ListenableFuture<R> flatMap(Function<? super T, ListenableFuture<R>> mapper, 
                                          Executor executor, 
                                          ListenerOptimizationStrategy optimizeExecution) {
    return FutureUtils.flatTransform(this, mapper, executor, optimizeExecution);
  }
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * <p>
   * The listener from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the runnable has high complexity, 
   * consider using {@link #addListener(Runnable, Executor)}.
   * 
   * @param listener the listener to run when the computation is complete
   */
  default void addListener(Runnable listener) {
    addListener(listener, null, null);
  }
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the listener will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * listener will execute immediately on the thread which is adding the listener.
   * 
   * @param listener the listener to run when the computation is complete
   * @param executor {@link Executor} the listener should be ran on, or {@code null}
   */
  default void addListener(Runnable listener, Executor executor) {
    addListener(listener, executor, null);
  }
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the listener will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * listener will execute immediately on the thread which is adding the listener.
   * <p>
   * Caution should be used when choosing to optimize the listener execution.  If the listener is 
   * complex, or wanting to be run concurrent, this optimization could prevent that.  In addition 
   * it will prevent other listeners from potentially being invoked until it completes.  However 
   * if the listener is small / fast, this can provide significant performance gains.  It should 
   * also be known that not all {@link ListenableFuture} implementations may be able to do such an 
   * optimization.  Please see {@link ListenerOptimizationStrategy} javadocs for more specific 
   * details of what optimizations are available.
   * 
   * @since 5.10
   * @param listener the listener to run when the computation is complete
   * @param executor {@link Executor} the listener should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   */
  public void addListener(Runnable listener, Executor executor, 
                          ListenerOptimizationStrategy optimizeExecution);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * <p>
   * The callback from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the callback has high complexity, 
   * consider passing an executor in for it to be called on.
   * 
   * @since 1.2.0
   * @param callback to be invoked when the computation is complete
   */
  default void addCallback(FutureCallback<? super T> callback) {
    addCallback(callback, null, null);
  }
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * 
   * @since 1.2.0
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   */
  default void addCallback(FutureCallback<? super T> callback, Executor executor) {
    addCallback(callback, executor, null);
  }
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * <p>
   * Caution should be used when choosing to optimize the listener execution.  If the listener is 
   * complex, or wanting to be run concurrent, this optimization could prevent that.  In addition 
   * it will prevent other listeners from potentially being invoked until it completes.  However 
   * if the listener is small / fast, this can provide significant performance gains.  It should 
   * also be known that not all {@link ListenableFuture} implementations may be able to do such an 
   * optimization.  Please see {@link ListenerOptimizationStrategy} javadocs for more specific 
   * details of what optimizations are available.
   * 
   * @since 5.10
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   */
  @SuppressWarnings("deprecation")
  default void addCallback(FutureCallback<? super T> callback, Executor executor, 
                           ListenerOptimizationStrategy optimizeExecution) {
    addListener(new RunnableFutureCallbackAdapter<>(this, callback), executor, optimizeExecution);
  }
}
