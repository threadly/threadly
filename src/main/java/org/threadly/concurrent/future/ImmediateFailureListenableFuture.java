package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * <p>Completed implementation of {@link ListenableFuture} that will immediately provide a failure 
 * condition.  Meaning listeners added will immediately be ran/executed, {@link FutureCallback}'s 
 * will immediately get called with the throwable provided, and {@link #get()} will immediately 
 * throw an {@link ExecutionException}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.3.0
 * @param <T> The result object type returned by this future
 */
public class ImmediateFailureListenableFuture<T> extends AbstractImmediateListenableFuture<T> {
  protected final Throwable failure;
  
  /**
   * Constructs a completed future with the provided failure.  If the failure is {@code null}, a 
   * new {@link Exception} will be created to represent it.
   * 
   * @param failure to be the cause of the ExecutionException from {@link #get()} calls
   */
  public ImmediateFailureListenableFuture(Throwable failure) {
    if (failure != null) {
      this.failure = failure;
    } else {
      this.failure = new Exception();
    }
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback) {
    callback.handleFailure(failure);
  }

  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor) {
    if (executor != null) {
      executor.execute(new CallbackInvokingTask(callback));
    } else {
      callback.handleFailure(failure);
    }
  }

  @Override
  public T get() throws ExecutionException {
    throw new ExecutionException(failure);
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws ExecutionException {
    throw new ExecutionException(failure);
  }
  
  /**
   * Small class to invoke callback with stored failure.
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
      callback.handleFailure(failure);
    }
  }
}