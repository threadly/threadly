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
  /**
   * Static instance of {@link ImmediateResultListenableFuture} which provides a {@code null} 
   * result.  Since this is a common case this can avoid GC overhead.  If you want to get this in 
   * any generic type use {@link FutureUtils#immediateResultFuture(Object)}, which when provided a 
   * {@code null} will always return this static instance.
   * 
   * @since 4.2.0
   */
  public static final ImmediateResultListenableFuture<?> NULL_RESULT;
  
  static {
    NULL_RESULT = new ImmediateResultListenableFuture<Object>(null);
  }
  
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
  public void addCallback(FutureCallback<? super T> callback) {
    callback.handleResult(result);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    if (executor != null) {
      executor.execute(new CallbackInvokingTask(callback));
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
  
  /**
   * Small class to invoke callback with stored result.
   *
   * @since 4.9.0
   */
  protected class CallbackInvokingTask implements Runnable {
    protected final FutureCallback<? super T> callback;
    
    public CallbackInvokingTask(FutureCallback<? super T> callback) {
      this.callback = callback;
    }
    
    @Override
    public void run() {
      callback.handleResult(result);
    }
  }
}