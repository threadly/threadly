package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * <p>Future where you can add a listener which is called once the future has completed.
 * The runnable will be called once the future completes either as a cancel, with result, 
 * or with an exception.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 * @param <T> type of result for future
 */
public interface ListenableFuture<T> extends Future<T> {
  /**
   * Add a listener to be called once the future has completed.  If the 
   * future has already finished, this will be called immediately.
   * 
   * The listener from this call will execute on the same thread the result was 
   * produced on, or on the adding thread if the future is already complete.  If 
   * the runnable has high complexity, consider passing an executor in for it 
   * to be ran on.
   * 
   * @param listener the listener to run when the computation is complete
   */
  public void addListener(Runnable listener);
  
  
  /**
   * Add a listener to be called once the future has completed.  If the 
   * future has already finished, this will be called immediately.
   * 
   * @param listener the listener to run when the computation is complete
   * @param executor executor the listener should be ran on
   */
  public void addListener(Runnable listener, Executor executor);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed. 
   * If the future has already finished, this will be called immediately.
   * 
   * The callback from this call will execute on the same thread the result was 
   * produced on, or on the adding thread if the future is already complete.
   * 
   * @param callback the callback to run when the computation is complete
   */
  public void addCallback(FutureCallback<? super T> callback);
  
  /**
   * Add a {@link FutureCallback} to be called once the future has completed. 
   * If the future has already finished, this will be called immediately.
   * 
   * The callback from this call will execute on the same thread the result was 
   * produced on, or on the adding thread if the future is already complete.  If 
   * the callback has high complexity, consider passing an executor in for it to 
   * be called on.
   * 
   * @param callback the callback to run when the computation is complete
   * @param executor executor the callback should be called on
   */
  public void addCallback(FutureCallback<? super T> callback, Executor executor);
}
