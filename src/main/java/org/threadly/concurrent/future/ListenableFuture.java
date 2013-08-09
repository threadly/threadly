package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Future where you can add a listener which is called once the future has completed.
 * The runnable will be called once the future completes either as a cancel, with result, 
 * or with an exception.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of result for future
 */
public interface ListenableFuture<T> extends Future<T> {
  /**
   * Add a listener to be called once the future has completed.  If the 
   * future has already finished, this will be called immediately.
   * 
   * Listeners from this call will execute on the same thread the result was 
   * produced on.
   * 
   * @param listener the listener to run when the computation is complete
   */
  public void addListener(Runnable listener);
  
  
  /**
   * Add a listener to be called once the future has completed.  If the 
   * future has already finished, this will be called immediately.
   * 
   * @param listener the listener to run when the computation is complete
   * @param executor executor listener should be called on
   */
  public void addListener(Runnable listener, Executor executor);
}
