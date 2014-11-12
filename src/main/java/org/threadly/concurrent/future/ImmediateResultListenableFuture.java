package org.threadly.concurrent.future;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * <p>Completed implementation of {@link ListenableFuture} that will immediately return a result.  
 * Meaning listeners added will immediately be ran/executed, {@link FutureCallback}'s will 
 * immediately get called with the result provided, and {@link #get()} calls will never block.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
public class ImmediateResultListenableFuture<T> extends AbstractImmediateListenableFuture<T> {
  private final T result;
  
  /**
   * Constructs a completed future that will return the provided result.
   * 
   * @param result Result that is returned by future
   */
  public ImmediateResultListenableFuture(T result) {
    this.result = result;
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    callback.handleResult(result);
  }

  @Override
  public void addCallback(final FutureCallback<? super T> callback, Executor executor) {
    if (executor != null) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          callback.handleResult(result);
        }
      });
    } else {
      callback.handleResult(result);
    }
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