package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>This class is the adapter between a {@link FutureCallback} and a {@link Runnable}.  Allowing 
 * you to supply this implementation as a Runnable as a listener into a {@link ListenableFuture} 
 * and have it convert the future's result into calls into a {@link FutureCallback}.</p>
 * 
 * <p>Instead of constructing this class, it is usually much easier to call into 
 * {@link FutureUtils#addCallback(ListenableFuture, FutureCallback)}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 3.2.0
 * @param <T> Type of result returned from the future
 */
public class RunnableFutureCallbackAdapter<T> implements Runnable {
  protected final Future<T> future;
  protected final FutureCallback<? super T> callback;
  
  /**
   * Constructs a new {@link RunnableFutureCallbackAdapter}.
   * 
   * @param future Future to get result or error from
   * @param callback Callback to call into once future has completed
   */
  public RunnableFutureCallbackAdapter(Future<T> future, FutureCallback<? super T> callback) {
    ArgumentVerifier.assertNotNull(future, "future");
    ArgumentVerifier.assertNotNull(callback, "callback");
    
    this.future = future;
    this.callback = callback;
  }

  @Override
  public void run() {
    try {
      T result = future.get();
      callback.handleResult(result);
    } catch (InterruptedException e) {
      // will not be possible if provided as a listener to a ListenableFuture
      Thread.currentThread().interrupt();
      callback.handleFailure(e);
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      callback.handleFailure(e.getCause());
    } catch (CancellationException e) {
      callback.handleFailure(e);
    }
  }
}
