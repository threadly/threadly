package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * <p>Future where you can add a listener which is called once the future has completed.  The 
 * runnable will be called once the future completes either as a cancel, with result, or with an 
 * exception.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> The result object type returned by this future
 */
public interface ListenableFuture<T> extends Future<T> {
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper.  
   * 
   * This can be easily used to chain together a series of operations, happening async (or in 
   * calling thread if already complete) until the final result is actually needed.  
   * {@link #map(Function, Executor)} can be used if transformation mapper is expensive and thus 
   * async execution is absolutely required.
   * 
   * If the future is complete already, the function may be invoked on the invoking thread.  If the 
   * future is not complete then the function will be invoked on the thread which completes 
   * the future (immediately after it completes).
   * 
   * @since 5.0.0
   * 
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @return A new {@link ListenableFuture} with the specified result type
   */
  public <R> ListenableFuture<R> map(Function<? super T, R> mapper);
  
  /**
   * Transform this future's result into another result by applying the provided mapper function.  
   * If this future completed in error, then the mapper will not be invoked, and instead the 
   * returned future will be completed in the same error state this future resulted in.  If the 
   * mapper function itself throws an Exception, then the returned future will result in the error 
   * thrown from the mapper.  
   * 
   * This can be easily used to chain together a series of operations, happening async until the 
   * final result is actually needed.  Once the future completes the mapper function will be invoked 
   * on the executor (if provided).  Because of that providing an executor can ensure this will 
   * never block.  If an executor is not provided then the mapper may be invoked on the calling 
   * thread (if the future is already complete), or on the same thread which the future completes 
   * on.  If the mapper function is very fast and cheap to run then {@link #map(Function)} or 
   * providing {@code null} for the executor can allow more efficient operation.
   * 
   * @since 5.0.0
   * 
   * @param <R> The type for the object returned from the mapper
   * @param mapper Function to invoke in order to transform the futures result
   * @param executor Executor to invoke mapper function on, or {@code null} 
   *          to invoke on this thread or future complete thread (depending on future state)
   * @return A new {@link ListenableFuture} with the specified result type
   */
  public <R> ListenableFuture<R> map(Function<? super T, R> mapper, Executor executor);
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * 
   * The listener from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the runnable has high complexity, 
   * consider using {@link #addListener(Runnable, Executor)}.
   * 
   * @param listener the listener to run when the computation is complete
   */
  public void addListener(Runnable listener);
  
  /**
   * Add a listener to be called once the future has completed.  If the future has already 
   * finished, this will be called immediately.
   * 
   * If the provided {@link Executor} is null, the listener will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * listener will execute immediately on the thread which is adding the listener.
   * 
   * @param listener the listener to run when the computation is complete
   * @param executor {@link Executor} the listener should be ran on, or {@code null}
   */
  public void addListener(Runnable listener, Executor executor);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * 
   * The callback from this call will execute on the same thread the result was produced on, or on 
   * the adding thread if the future is already complete.  If the callback has high complexity, 
   * consider passing an executor in for it to be called on.
   * 
   * @since 1.2.0
   * 
   * @param callback to be invoked when the computation is complete
   */
  public void addCallback(FutureCallback<? super T> callback);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed.  If the future has 
   * already finished, this will be called immediately.
   * 
   * If the provided {@link Executor} is null, the callback will execute on the thread which 
   * computed the original future (once it is done).  If the future has already completed, the 
   * callback will execute immediately on the thread which is adding the callback.
   * 
   * @since 1.2.0
   * 
   * @param callback to be invoked when the computation is complete
   * @param executor {@link Executor} the callback should be ran on, or {@code null}
   */
  public void addCallback(FutureCallback<? super T> callback, Executor executor);
}
