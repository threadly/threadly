package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * This class is designed to be a helper when returning a single result asynchronously.  This is 
 * particularly useful if this result is produced over multiple threads (and thus the scheduler 
 * returned future is not useful).
 * 
 * @since 1.2.0
 * @param <T> The result object type returned by this future
 */
public class SettableListenableFuture<T> extends AbstractCompletableListenableFuture<T>
                                         implements ListenableFuture<T>, FutureCallback<T> {
  protected final boolean throwIfAlreadyComplete;
  private volatile String cancelStateMessage;
  
  /**
   * Constructs a new {@link SettableListenableFuture}.  You can return this immediately and 
   * provide a result to the object later when it is ready.  
   * <p>
   * This defaults in the behavior since version 1.2.0 where if the future has completed (either 
   * by {@link #cancel(boolean)}, {@link #setResult(Object)}, or {@link #setFailure(Throwable)}), 
   * any additional attempts to {@link #setResult(Object)} or {@link #setFailure(Throwable)} will 
   * result in a {@link IllegalStateException} being thrown.
   */
  public SettableListenableFuture() {
    this(true);
  }
  
  /**
   * Constructs a new {@link SettableListenableFuture}.  You can return this immediately and 
   * provide a result to the object later when it is ready.  
   * <p>
   * This constructor allows you to control the behavior when results are attempt to be set after 
   * the future has already completed (either by 
   * {@link #cancel(boolean)}, {@link #setResult(Object)}, or {@link #setFailure(Throwable)}).  
   * <p>
   * If {@code true}, any additional attempts to {@link #setResult(Object)} or 
   * {@link #setFailure(Throwable)} will result in a {@link IllegalStateException} being thrown.  
   * <p>
   * If {@code false}, additional attempts to set a result will just be silently ignored.
   * 
   * @param throwIfAlreadyComplete Defines the behavior when result or failure is set on a completed future
   */
  public SettableListenableFuture(boolean throwIfAlreadyComplete) {
    this(throwIfAlreadyComplete, null);
  }
  
  /**
   * Constructs a new {@link SettableListenableFuture}.  You can return this immediately and 
   * provide a result to the object later when it is ready.  
   * <p>
   * This constructor allows you to control the behavior when results are attempt to be set after 
   * the future has already completed (either by 
   * {@link #cancel(boolean)}, {@link #setResult(Object)}, or {@link #setFailure(Throwable)}).  
   * <p>
   * If {@code true}, any additional attempts to {@link #setResult(Object)} or 
   * {@link #setFailure(Throwable)} will result in a {@link IllegalStateException} being thrown.  
   * <p>
   * If {@code false}, additional attempts to set a result will just be silently ignored.
   * 
   * @param throwIfAlreadyComplete Defines the behavior when result or failure is set on a completed future
   * @param executingExecutor Executor this future will complete on, used for optimizations
   */
  protected SettableListenableFuture(boolean throwIfAlreadyComplete, Executor executingExecutor) {
    super(executingExecutor);
    
    this.throwIfAlreadyComplete = throwIfAlreadyComplete;
    cancelStateMessage = null;
  }
  
  /**
   * This call defers to {@link #setResult(Object)}.  It is implemented so that you can construct 
   * this, return it immediately, but only later provide this as a callback to another 
   * {@link ListenableFuture} implementation.  
   * <p>
   * This should never be invoked by the implementor, this should only be invoked by other 
   * {@link ListenableFuture}'s through the use of providing this to 
   * {@link ListenableFuture#callback(FutureCallback)}.  
   * <p>
   * If this is being used to chain together {@link ListenableFuture}'s, 
   * {@link #setResult(Object)}/{@link #setFailure(Throwable)} should never be called manually (or 
   * an exception will occur).
   * 
   * @param result Result object to provide to the future to be returned from {@link #get()} call
   */
  @Override
  public void handleResult(T result) {
    // copied from setResult to keep stack sizes for mapping futures short
    if (! completeWithResult(result) && throwIfAlreadyComplete) {
      throw new IllegalStateException("Future already done");
    }
  }
  
  /**
   * This call defers to {@link #setFailure(Throwable)}.  It is implemented so that you can 
   * construct this, return it immediately, but only later provide this as a callback to another 
   * {@link ListenableFuture} implementation.  If the parent chained future was canceled this 
   * implementation will attempt to cancel the future as well.
   * <p>
   * This should never be invoked by the implementor, this should only be invoked by other 
   * {@link ListenableFuture}'s through the use of providing this to 
   * {@link ListenableFuture#callback(FutureCallback)}.  
   * <p>
   * If this is being used to chain together {@link ListenableFuture}'s, 
   * {@link #setResult(Object)}/{@link #setFailure(Throwable)} should never be called manually (or 
   * an exception will occur).
   * 
   * @param t Throwable to be provided as the cause from the ExecutionException thrown from {@link #get()} call
   */
  @Override
  public void handleFailure(Throwable t) {
    if (isDone()) {
      return; // shortcut
    }
    if (t instanceof CancellationException) {
      if (cancelStateMessage == null) {
        cancelStateMessage = t.getMessage();
        boolean interrupt = Thread.currentThread().isInterrupted();
        if (! cancel(interrupt)) {
          // try to make sure overriding cancel methods are not causing the issue
          super.cancel(interrupt);
        }
      }
    } else {
      // copied from setFailure to keep stack sizes for mapping futures short
      if (! completeWithFailure(t) && throwIfAlreadyComplete) {
        throw new IllegalStateException("Future already done", t);
      }
    }
  }
  
  /**
   * Call to indicate this future is done, and provide the given result.  It is expected that only 
   * this or {@link #setFailure(Throwable)} are called.
   * <p>
   * If future has already completed and constructed with {@link #SettableListenableFuture()} or 
   * {@code true} provided to {@link #SettableListenableFuture(boolean)} this will throw an 
   * {@link IllegalStateException}.  If complete but constructed with a {@code false} this result 
   * will be ignored.
   * 
   * @param result result to provide for {@link #get()} call, can be {@code null}
   * @return {@code true} if the result was set (ie future did not complete in failure or cancel}
   */
  public boolean setResult(T result) {
    boolean set = completeWithResult(result);
    if (throwIfAlreadyComplete && ! set) {
      throw new IllegalStateException("Future already done");
    } else {
      return set;
    }
  }
  
  /**
   * Call to indicate this future is done, and provide the occurred failure.  It is expected that 
   * only this or {@link #setResult(Object)} are called, and only called once.  If the provided 
   * failure is {@code null}, a new {@link Exception} will be created so that something is always 
   * provided in the {@link ExecutionException} on calls to {@link #get()}.
   * <p>
   * If the future has already completed and constructed with {@link #SettableListenableFuture()} 
   * or {@code true} provided to {@link #SettableListenableFuture(boolean)} this will throw an 
   * {@link IllegalStateException}.  If complete but constructed with a {@code false} this failure 
   * result will be ignored.
   * 
   * @param failure Throwable that caused failure during computation.
   * @return {@code true} if the failure was set (ie future did not complete with result or cancel}
   */
  public boolean setFailure(Throwable failure) {
    boolean set = completeWithFailure(failure);
    if (throwIfAlreadyComplete && ! set) {
      throw new IllegalStateException("Future already done", failure);
    } else {
      return set;
    }
  }
  
  /**
   * Optional call to set the thread internally that will be generating the result for this 
   * future.  Setting this thread allows it so that if a {@link #cancel(boolean)} call is invoked 
   * with {@code true}, we can send an interrupt to this thread.  
   * <p>
   * The reference to this thread will be cleared after this future has completed (thus allowing 
   * it to be garbage collected).
   * 
   * @param thread Thread that is generating the result for this future
   */
  public void setRunningThread(Thread thread) {
    if (! isDone()) {
      execThread = thread;
    }
  }
  
  @Override
  protected String getCancellationExceptionMessage() {
    return cancelStateMessage;
  }

  @Override
  protected void handleCompleteState() {
    // we must unset the execThread as the abstract class does not
    execThread = null;
  }
}