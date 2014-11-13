package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>This is a slight modification to the behavior of the parent class 
 * {@link SettableListenableFuture}.  In this implementation, after the first {@link #get()} call 
 * the stored result will be cleared (thus allowing it to be available for garbage collection).</p>
 * 
 * <p>Because the result is cleared after the first {@link #get()} call, {@link #get()} can ONLY 
 * BE INVOKED ONCE.  Additional invocations will result in a {@link IllegalStateException}.  This 
 * implementation only makes sense in specific uses cases (for example when chaining 
 * {@link SettableListenableFuture}'s together).</p>
 * 
 * <p>In addition only one callback can be added, and if a callback is added you can not call 
 * {@link #get()}.  That single callback will clear the result (functioning like a {@link #get()} 
 * call).</p>
 * 
 * @author jent - Mike Jensen
 * @since 3.3.0
 * @param <T> The result object type returned by this future
 */
public class ClearOnGetSettableListenableFuture<T> extends SettableListenableFuture<T> {
  private final AtomicBoolean callbackAdded = new AtomicBoolean(false);

  /**
   * This callback functions the same as described in 
   * {@link ListenableFuture#addCallback(FutureCallback)}, with one major exception.  Only one 
   * callback can be added into this implementation.  If additional callbacks are attempted to be 
   * added an exception will be thrown.  
   * 
   * This callback will clear the result, so if a callback is used {@link #get()} CAN NOT BE INVOKED.
   * 
   * @param callback to be invoked when the computation is complete
   */
  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    addCallback(callback, null);
  }

  /**
   * This callback functions the same as described in 
   * {@link ListenableFuture#addCallback(FutureCallback, Executor)}, with one major exception.  
   * Only one callback can be added into this implementation.  If additional callbacks are 
   * attempted to be added an exception will be thrown.  
   * 
   * This callback will clear the result, so if a callback is used {@link #get()} CAN NOT BE INVOKED.
   * 
   * @param callback to be invoked when the computation is complete
   */
  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    if (! callbackAdded.compareAndSet(false, true)) {
      throw new IllegalStateException("Callback already added");
    }
    
    super.addCallback(callback, executor);
  }
  
  /**
   * This invocation behaves the same as described in {@link ListenableFuture#get()} with one 
   * major exception.  After calling this, the internal stored result will be cleared.  Future 
   * attempts to invoke {@link #get()} will result in an exception to be thrown.  
   * 
   * If a callback was added, that operates as the single allowed {@link #get()} call, invoking 
   * this in addition to adding a callback will result in one of the two operations resulting in 
   * an error.
   * 
   * @return The result from the future once it is ready
   * @throws InterruptedException Thrown if the thread is interrupted while waiting for a result
   * @throws ExecutionException Thrown if the internal computation resulted in an error
   */
  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      T result = super.get();
    
      return result;
    } finally {
      clearResult();
    }
  }

  /**
   * This invocation behaves the same as described in {@link ListenableFuture#get(long, TimeUnit)} 
   * with one major exception.  After calling this, the internal stored result will be cleared.  
   * Future attempts to invoke {@link #get(long, TimeUnit)} will result in an exception to be 
   * thrown.  
   * 
   * If a callback was added, that operates as the single allowed {@link #get()} call, invoking 
   * this in addition to adding a callback will result in one of the two operations resulting in 
   * an error.
   * 
   * @return The result from the future once it is ready
   * @throws InterruptedException Thrown if the thread is interrupted while waiting for a result
   * @throws ExecutionException Thrown if the internal computation resulted in an error
   */
  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                   ExecutionException,
                                                   TimeoutException {
    if (callbackAdded.get()) {
      throw new IllegalStateException("Callback added, can't call .get()");
    }
    
    boolean timeoutThrown = false;
    try {
      T result = super.get(timeout, unit);
      
      return result;
    } catch (TimeoutException e) {
      timeoutThrown = true;
      throw e;
    } finally {
      // we can't clear if we timed out, because the result was never set
      if (! timeoutThrown) {
        clearResult();
      }
    }
  }
}
