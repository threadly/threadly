package org.threadly.concurrent.future;

import static org.threadly.concurrent.future.InternalFutureUtils.invokeCompletedDirectly;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Consumer;
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
     * If the future has already completed, this optimization will ignore the executor passed in 
     * and will instead invoke the listener / callback on the invoking thread trying to add the 
     * listener.  This is similar to {@link #SingleThreadIfExecutorMatchOrDone} except that it will 
     * still execute out on the executor if they match (which facilitates cases where  concurrency 
     * of multiple threads on the same pool is desired).
     */
    InvokingThreadIfDone,
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
    return InternalFutureUtils.transform(this, null, mapper, true, null, null);
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
    return InternalFutureUtils.transform(this, null, mapper, true, executor, null);
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
    return InternalFutureUtils.transform(this, null, mapper, true, executor, optimizeExecution);
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
    return InternalFutureUtils.transform(this, null, mapper, false, null, null);
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
    return InternalFutureUtils.transform(this, null, mapper, false, executor, null);
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
    return InternalFutureUtils.transform(this, null, mapper, false, executor, optimizeExecution);
  }
  
  /**
   * Convenience function for mapping this future in with an existing future, ignoring the result 
   * of this current future.  This is equivalent to {@code flatMap((ignored) -> future)}, and 
   * conceptually the same as {@link FutureUtils#makeFailurePropagatingCompleteFuture(Iterable)} 
   * with both futures being provided (though this allows us to capture the result of the provided 
   * future).
   * <p>
   * Please be aware that {@code flatMap(futureProducer.get())} is NOT equivalent to 
   * {@code flatMap((ignored) -> futureProducer.get())}.  As the second version would delay starting 
   * the future generation until this future completes.  By calling into this with a future you will 
   * be starting its execution immediately.
   * 
   * @param <R> The type of result returned from the provided future
   * @param future The future to flat mpa against this one
   * @return A new {@link ListenableFuture} that will complete when both this and the provided future does
   */
  default <R> ListenableFuture<R> flatMap(ListenableFuture<R> future) {
    return InternalFutureUtils.flatTransform(this, null, (ignored) -> future, null, null);
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
    return InternalFutureUtils.flatTransform(this, null, mapper, null, null);
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
    return InternalFutureUtils.flatTransform(this, null, mapper, executor, null);
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
    return InternalFutureUtils.flatTransform(this, null, mapper, executor, optimizeExecution);
  }
  
  /**
   * Similar to {@link #throwMap(Function)} except this mapper will only be invoked when the 
   * future is in a failure state (from either the original computation or an earlier mapper 
   * throwing an exception).  If this future does resolve in a failure state, and that exception 
   * class matches the one provided here.  The mapper function will then be provided that 
   * throwable, it can then map that throwable back into a result (perhaps an {@code Optional}), 
   * or re-throw either the same or a different exception keep the future in a failure state.  If 
   * the future completes with a normal result, this mapper will be ignored, and the result will 
   * be forwarded on without invoking this mapper.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper The mapper to convert a thrown exception to either a result or thrown exception
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                                Function<? super TT, ? extends T> mapper) {
    return InternalFutureUtils.failureTransform(this, null, mapper, throwableType, null, null);
  }

  /**
   * Similar to {@link #throwMap(Function, Executor)} except this mapper will only be invoked when 
   * the future is in a failure state (from either the original computation or an earlier mapper 
   * throwing an exception).  If this future does resolve in a failure state, and that exception 
   * class matches the one provided here.  The mapper function will then be provided that 
   * throwable, it can then map that throwable back into a result (perhaps an {@code Optional}), 
   * or re-throw either the same or a different exception keep the future in a failure state.  If 
   * the future completes with a normal result, this mapper will be ignored, and the result will 
   * be forwarded on without invoking this mapper.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper The mapper to convert a thrown exception to either a result or thrown exception
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                                Function<? super TT, ? extends T> mapper, 
                                                                Executor executor) {
    return InternalFutureUtils.failureTransform(this, null, mapper, throwableType, executor, null);
  }

  /**
   * Similar to {@link #throwMap(Function, Executor, ListenerOptimizationStrategy)} except this 
   * mapper will only be invoked when the future is in a failure state (from either the original 
   * computation or an earlier mapper throwing an exception).  If this future does resolve in a 
   * failure state, and that exception class matches the one provided here.  The mapper function 
   * will then be provided that throwable, it can then map that throwable back into a result 
   * perhaps an {@code Optional}), or re-throw either the same or a different exception keep the 
   * future in a failure state.  If the future completes with a normal result, this mapper will be 
   * ignored, and the result will be forwarded on without invoking this mapper.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper The mapper to convert a thrown exception to either a result or thrown exception
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> mapFailure(Class<TT> throwableType, 
                                                                Function<? super TT, ? extends T> mapper, 
                                                                Executor executor, 
                                                                ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.failureTransform(this, null, mapper, throwableType, 
                                                executor, optimizeExecution);
  }

  /**
   * Similar to {@link #mapFailure(Class, Function)} except that this mapper function returns a 
   * {@link ListenableFuture} if it needs to map the Throwable / failure into a result or another 
   * failure.  The mapper function can return a Future that will (or may) provide a result, or it 
   * can provide a future that will result in the same or another failure.  Similar to  
   * {@link #mapFailure(Class, Function)} the mapper can also throw an exception directly.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper Function to invoke in order to transform the futures result
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                    Function<? super TT, ListenableFuture<T>> mapper) {
    return InternalFutureUtils.flatFailureTransform(this, null, mapper, throwableType, null, null);
  }

  /**
   * Similar to {@link #mapFailure(Class, Function, Executor)} except that this mapper function 
   * returns a {@link ListenableFuture} if it needs to map the Throwable / failure into a result 
   * or another failure.  The mapper function can return a Future that will (or may) provide a 
   * result, or it can provide a future that will result in the same or another failure.  Similar 
   * to {@link #mapFailure(Class, Function, Executor)} the mapper can also throw an exception 
   * directly.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                    Function<? super TT, ListenableFuture<T>> mapper, 
                                                                    Executor executor) {
    return InternalFutureUtils.flatFailureTransform(this, null, mapper, throwableType, 
                                                    executor, null);
  }

  /**
   * Similar to {@link #mapFailure(Class, Function, Executor, ListenerOptimizationStrategy)} except 
   * that this mapper function returns a {@link ListenableFuture} if it needs to map the Throwable 
   * into a result or another failure.  The mapper function can return a Future that will (or may) 
   * provide a result, or it can provide a future that will result in the same or another failure.  
   * Similar to {@link #mapFailure(Class, Function, Executor, ListenerOptimizationStrategy)} the 
   * mapper can also throw an exception directly.
   * 
   * @since 5.17
   * @param <TT> The type of throwable that should be handled
   * @param throwableType The class referencing to the type of throwable this mapper handles
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return A {@link ListenableFuture} that will resolve after the mapper is considered
   */
  default <TT extends Throwable> ListenableFuture<T> flatMapFailure(Class<TT> throwableType, 
                                                                    Function<? super TT, ListenableFuture<T>> mapper, 
                                                                    Executor executor, 
                                                                    ListenerOptimizationStrategy optimizeExecution) {
    return InternalFutureUtils.flatFailureTransform(this, null, mapper, throwableType, 
                                                    executor, optimizeExecution);
  }
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * <p>
   * The listener from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the runnable has high complexity, 
   * consider using {@link #listener(Runnable, Executor)}.
   * 
   * @since 5.34
   * @param listener the listener to run when the computation is complete
   * @return Exactly {@code this} instance to add more listeners or other functional operations
   */
  default ListenableFuture<T> listener(Runnable listener) {
    return listener(listener, null, null);
  }
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the listener will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * listener will execute immediately on the thread which is adding the listener.
   * 
   * @since 5.34
   * @param listener the listener to run when the computation is complete
   * @param executor {@link Executor} the listener should be ran on, or {@code null}
   * @return Exactly {@code this} instance to add more listeners or other functional operations
   */
  default ListenableFuture<T> listener(Runnable listener, Executor executor) {
    return listener(listener, executor, null);
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
   * @since 5.34
   * @param listener the listener to run when the computation is complete
   * @param executor {@link Executor} the listener should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return Exactly {@code this} instance to add more listeners or other functional operations
   */
  public ListenableFuture<T> listener(Runnable listener, Executor executor, 
                                      ListenerOptimizationStrategy optimizeExecution);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * <p>
   * The callback from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the callback has high complexity, 
   * consider passing an executor in for it to be called on.
   * <p>
   * If you only care about the success result case please see {@link #resultCallback(Consumer)} or 
   * conversely if you only want to be invoked for failure cases please see 
   * {@link #failureCallback(Consumer)}.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> callback(FutureCallback<? super T> callback) {
    return callback(callback, null, null);
  }
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * <p>
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * <p>
   * If you only care about the success result case please see 
   * {@link #resultCallback(Consumer, Executor)} or conversely if you only want to be invoked for 
   * failure cases please see {@link #failureCallback(Consumer, Executor)}.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor) {
    return callback(callback, executor, null);
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
   * <p>
   * If you only care about the success result case please see 
   * {@link #resultCallback(Consumer, Executor, ListenerOptimizationStrategy)} or conversely if you 
   * only want to be invoked for failure cases please see 
   * {@link #failureCallback(Consumer, Executor, ListenerOptimizationStrategy)}.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> callback(FutureCallback<? super T> callback, Executor executor, 
                                       ListenerOptimizationStrategy optimizeExecution) {
    if (invokeCompletedDirectly(executor, optimizeExecution) && isDone()) {
      // no need to construct anything, just invoke directly
      try {
        callback.handleResult(get());
      } catch (ExecutionException e) {
        callback.handleFailure(e.getCause());
      } catch (CancellationException e) {
        callback.handleFailure(e);
      } catch (InterruptedException e) { // should not be possible
        callback.handleFailure(e);
      }
    } else {
      listener(() -> {
        try {
          callback.handleResult(get());
        } catch (ExecutionException e) {
          callback.handleFailure(e.getCause());
        } catch (CancellationException e) {
          callback.handleFailure(e);
        } catch (InterruptedException e) { // should not be possible
          callback.handleFailure(e);
        }
      }, executor, optimizeExecution);
    }
    
    return this;
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed without 
   * error, the result will be provided to the {@link Consumer}, otherwise it will go un-invoked.
   * <p>
   * The callback from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the callback has high complexity, 
   * consider passing an executor in for it to be called on.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> resultCallback(Consumer<? super T> callback) {
    return resultCallback(callback, null, null);
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed without 
   * error, the result will be provided to the {@link Consumer}, otherwise it will go un-invoked.
   * <p>
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor) {
    return resultCallback(callback, executor, null);
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed without 
   * error, the result will be provided to the {@link Consumer}, otherwise it will go un-invoked.
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
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> resultCallback(Consumer<? super T> callback, Executor executor, 
                                             ListenerOptimizationStrategy optimizeExecution) {
    if (invokeCompletedDirectly(executor, optimizeExecution) && isDone()) {
      if (! isCancelled()) {
        // no need to construct anything, just invoke directly
        try {
          callback.accept(get());
        } catch (ExecutionException e) {
          // ignored
        } catch (InterruptedException e) {
          // should not be possible
        }
      }
    } else {
      listener(() -> {
        if (! isCancelled()) {
          try {
            callback.accept(get());
          } catch (ExecutionException e) {
            // ignored
          } catch (InterruptedException e) {
            // should not be possible
          }
        }
      }, executor, optimizeExecution);
    }
    
    return this;
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed with an 
   * error, the {@link Throwable} will be provided to the {@link Consumer}, otherwise if no error 
   * occurred, the callback will go un-invoked.
   * <p>
   * The callback from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the callback has high complexity, 
   * consider passing an executor in for it to be called on.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> failureCallback(Consumer<Throwable> callback) {
    return failureCallback(callback, null, null);
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed with an 
   * error, the {@link Throwable} will be provided to the {@link Consumer}, otherwise if no error 
   * occurred, the callback will go un-invoked.
   * <p>
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * 
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor) {
    return failureCallback(callback, executor, null);
  }
  
  /**
   * Add a {@link Consumer} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.  Assuming the future has completed with an 
   * error, the {@link Throwable} will be provided to the {@link Consumer}, otherwise if no error 
   * occurred, the callback will go un-invoked.
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
   * @since 5.34
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   * @param optimizeExecution {@code true} to avoid listener queuing for execution if already on the desired pool
   * @return Exactly {@code this} instance to add more callbacks or other functional operations
   */
  default ListenableFuture<T> failureCallback(Consumer<Throwable> callback, Executor executor, 
                                              ListenerOptimizationStrategy optimizeExecution) {
    if (invokeCompletedDirectly(executor, optimizeExecution) && isDone()) {
      // no need to construct anything, just invoke directly
      try {
        get();  // ignore result if provided
      } catch (ExecutionException e) {
        callback.accept(e.getCause());
      } catch (CancellationException e) {
        callback.accept(e);
      } catch (InterruptedException e) { // should not be possible
        callback.accept(e);
      }
    } else {
      listener(() -> {
        try {
          get();  // ignore result if provided
        } catch (ExecutionException e) {
          callback.accept(e.getCause());
        } catch (CancellationException e) {
          callback.accept(e);
        } catch (InterruptedException e) { // should not be possible
          callback.accept(e);
        }
      }, executor, optimizeExecution);
    }
    
    return this;
  }
  
  /**
   * A best effort to return the stack trace for for the executing thread of either this future, 
   * or a future which this depends on through the use of {@link #map(Function)} or similar 
   * functions.  If there is no thread executing the future yet, or the future has already 
   * completed, then this will return {@code null}. 
   * <p>
   * This is done without locking (though generating a stack trace still requires a JVM safe point), 
   * so the resulting stack trace is NOT guaranteed to be accurate.  In most cases (particularly 
   * when blocking) this should be accurate though.
   * 
   * @return The stack trace currently executing the future, or {@code null} if unavailable
   */
  public StackTraceElement[] getRunningStackTrace();
}
