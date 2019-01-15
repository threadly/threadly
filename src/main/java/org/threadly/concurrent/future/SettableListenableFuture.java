package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.threadly.concurrent.event.RunnableListenerHelper;
import org.threadly.util.Clock;
import org.threadly.util.StringUtils;

/**
 * This class is designed to be a helper when returning a single result asynchronously.  This is 
 * particularly useful if this result is produced over multiple threads (and thus the scheduler 
 * returned future is not useful).
 * 
 * @since 1.2.0
 * @param <T> The result object type returned by this future
 */
public class SettableListenableFuture<T> extends AbstractCancellationMessageProvidingListenableFuture<T>
                                         implements ListenableFuture<T>, FutureCallback<T> {
  // used to represent a canceled state when no other message is available
  private static final String EMPTY_CANCEL_STATE_MESSAGE = StringUtils.makeRandomString(64);
  
  protected final RunnableListenerHelper listenerHelper;
  protected final Object resultLock;
  protected final boolean throwIfAlreadyComplete;
  protected volatile Thread runningThread;
  private volatile boolean done;
  private volatile String cancelStateMessage;  // set non-null when canceled
  private Executor executingExecutor; // since done is volatile, this does not need to be
  private boolean resultCleared;
  private T result;
  private Throwable failure;
  
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
    this.listenerHelper = new RunnableListenerHelper(true);
    this.resultLock = listenerHelper; // cheating to avoiding another object just for a lock
    this.throwIfAlreadyComplete = throwIfAlreadyComplete;
    this.runningThread = null;
    this.executingExecutor = executingExecutor;
    done = false;
    cancelStateMessage = null;
    resultCleared = false;
    result = null;
    failure = null;
  }

  @Override
  public void addListener(Runnable listener, Executor executor, 
                          ListenerOptimizationStrategy optimize) {
    listenerHelper.addListener(listener, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 null : executor, 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
  }
  
  @Override
  public void addCallback(FutureCallback<? super T> callback, Executor executor, 
                          ListenerOptimizationStrategy optimize) {
    if (executor == null | optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone) {
      // can't check `done` without synchronizing, but we can check final states optimistically
      // because a `null` result requires us to check `done` (which needs to synchronize or we may 
      // see an inconsistent final state), this only works for non-null results
      if (result != null) {
        callback.handleResult(result);
        return;
      } else if (failure != null) {
        callback.handleFailure(failure);
        return;
      } else if (cancelStateMessage != null) {
        callback.handleFailure(new CancellationException(getCancellationExceptionMessage()));
        return;
      }
    }
    // This allows us to avoid synchronization (since listeners wont be invoked till final / 
    // result state has all be set and synced).  So this allows us to avoid synchronization (which 
    // is important as we don't want to hold the lock while invoking into the callback
    listenerHelper.addListener(() -> {
                                 if (failure != null) {
                                   callback.handleFailure(failure);
                                 } else if (cancelStateMessage != null) {
                                   callback.handleFailure(new CancellationException(getCancellationExceptionMessage()));
                                 } else {
                                   callback.handleResult(result);
                                 }
                               }, 
                               executor == executingExecutor && 
                                   (optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone | 
                                    optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatch) ? 
                                 null : executor, 
                               optimize == ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone ? 
                                 null : executor);
  }
  
  /**
   * This call defers to {@link #setResult(Object)}.  It is implemented so that you can construct 
   * this, return it immediately, but only later provide this as a callback to another 
   * {@link ListenableFuture} implementation.  
   * <p>
   * This should never be invoked by the implementor, this should only be invoked by other 
   * {@link ListenableFuture}'s.  
   * <p>
   * If this is being used to chain together {@link ListenableFuture}'s, 
   * {@link #setResult(Object)}/{@link #setFailure(Throwable)} should never be called manually (or 
   * an exception will occur).
   * 
   * @param result Result object to provide to the future to be returned from {@link #get()} call
   */
  @Override
  public void handleResult(T result) {
    setResult(result);
  }
  
  /**
   * This call defers to {@link #setFailure(Throwable)}.  It is implemented so that you can 
   * construct this, return it immediately, but only later provide this as a callback to another 
   * {@link ListenableFuture} implementation.  If the parent chained future was canceled this 
   * implementation will attempt to cancel the future as well.
   * <p>
   * This should never be invoked by the implementor, this should only be invoked by other 
   * {@link ListenableFuture}'s.  
   * <p>
   * If this is being used to chain together {@link ListenableFuture}'s, 
   * {@link #setResult(Object)}/{@link #setFailure(Throwable)} should never be called manually (or 
   * an exception will occur).
   * 
   * @param t Throwable to be provided as the cause from the ExecutionException thrown from {@link #get()} call
   */
  @Override
  public void handleFailure(Throwable t) {
    if (t instanceof CancellationException) {
      if (cancelStateMessage == null) {
        cancelStateMessage = t.getMessage() == null ? EMPTY_CANCEL_STATE_MESSAGE : t.getMessage();
        boolean interrupt = Thread.currentThread().isInterrupted();
        if (! cancel(interrupt)) {
          // may have been overriden, go ahead and complete the future into a cancel state
          internalCancel(interrupt);
        }
      }
    } else {
      setFailure(t);
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
    synchronized (resultLock) {
      if (! setDone(null)) {
        return false;
      }
      
      this.result = result;
    }
    
    // call outside of lock
    listenerHelper.callListeners();
    runningThread = null;
    
    return true;
  }
  
  /**
   * Call to indicate this future is done, and provide the occurred failure.  It is expected that 
   * only this or {@link #setResult(Object)} are called, and only called once.  If the provided 
   * failure is {@code null}, a new {@link Exception} will be created so that something is always 
   * provided in the {@link ExecutionException} on calls to {@link #get()}.
   * <p>
   * If future has already completed and constructed with {@link #SettableListenableFuture()} or 
   * {@code true} provided to {@link #SettableListenableFuture(boolean)} this will throw an 
   * {@link IllegalStateException}.  If complete but constructed with a {@code false} this failure 
   * result will be ignored.
   * 
   * @param failure Throwable that caused failure during computation.
   * @return {@code true} if the failure was set (ie future did not complete with result or cancel}
   */
  public boolean setFailure(Throwable failure) {
    synchronized (resultLock) {
      if (! setDone(failure)) { // if failure is null, there is no point to have the stack twice
        return false;
      }

      if (failure == null) {
        failure = new Exception();
      }
      this.failure = failure;
    }
    
    // call outside of lock
    listenerHelper.callListeners();
    runningThread = null;
    
    return true;
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
    if (! done) {
      this.runningThread = thread;
    }
  }

  @Override
  public boolean cancel(boolean interruptThread) {
    return internalCancel(interruptThread);
  }
  
  /**
   * This internal cancel function ensures we can set the class into a canceled state even if the 
   * {@link #cancel(boolean)} function is overriden.  However this should only be invoked after 
   * {@link #cancel(boolean)} was tried initially (so overriding classes can handle cancel logic 
   * they may require).
   * 
   * @param interruptThread If {@code true} send an interrupt to the processing thread
   * @return {@code true} if this future was transitioned to a canceled state
   */
  private boolean internalCancel(boolean interruptThread) {
    if (done) {
      return false;
    }
    
    boolean canceled;
    synchronized (resultLock) {
      canceled = ! done;
      if (canceled) {
        if (interruptThread) {
          Thread runningThread = this.runningThread;
          if (runningThread != null) {
            runningThread.interrupt();
          }
        }
        
        setCanceled();
      }
    }
    
    if (canceled) {
      // call outside of lock
      listenerHelper.callListeners();
      runningThread = null;
    }
    
    return canceled;
  }

  @Override
  public boolean isCancelled() {
    return cancelStateMessage != null;
  }
  
  /**
   * Clears the stored result from this set future.  This allows the result to be available for 
   * garbage collection.  After this call, future calls to {@link #get()} will throw an 
   * {@link IllegalStateException}.  So it is critical that this is only called after you are sure 
   * no future calls to get the result on this future will be attempted.
   * <p>
   * The design of this is so that if you want to chain {@link ListenableFuture}'s together, you 
   * can clear the results of old ones after their result has been consumed.  This is really only 
   * useful in very specific instances.
   */
  public void clearResult() {
    synchronized (resultLock) {
      if (! done) {
        throw new IllegalStateException("Result not set yet");
      }
      resultCleared = true;
      result = null;
      failure = null;
    }
  }

  // MUST be synchronized on resultLock before calling
  protected void setCanceled() {
    if (this.cancelStateMessage == null) { // may have been set earlier in the cancel process
      this.cancelStateMessage = EMPTY_CANCEL_STATE_MESSAGE;
    }
    
    setDone(null);
  }
  
  // MUST be synchronized on resultLock before calling
  protected boolean setDone(Throwable cause) {
    if (done) {
      if (throwIfAlreadyComplete) {
        throw new IllegalStateException("Future already done", cause);
      }
      return false;
    }
    
    done = true;
    executingExecutor = null;
    
    resultLock.notifyAll();
    return true;
  }

  @Override
  public boolean isDone() {
    return done;
  }
  
  @Override
  protected String getCancellationExceptionMessage() {
    String result = cancelStateMessage;
    if (result == EMPTY_CANCEL_STATE_MESSAGE) { // identity check is best here
      return null;
    }
    return result;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    synchronized (resultLock) {
      while (! done) {
        resultLock.wait();
      }
      
      if (failure != null) {
        throw new ExecutionException(failure);
      } else if (cancelStateMessage != null) {
        throw new CancellationException(getCancellationExceptionMessage());
      } else if (resultCleared) {
        throw new IllegalStateException("Result cleared, future get's not possible");
      } else {
        return result;
      }
    }
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                   ExecutionException, TimeoutException {
    long startTime = Clock.accurateForwardProgressingMillis();
    long timeoutInMs = unit.toMillis(timeout);
    synchronized (resultLock) {
      long remainingInMs;
      while (! done && 
             (remainingInMs = timeoutInMs - (Clock.accurateForwardProgressingMillis() - startTime)) > 0) {
        resultLock.wait(remainingInMs);
      }
      
      if (failure != null) {
        throw new ExecutionException(failure);
      } else if (cancelStateMessage != null) {
        throw new CancellationException(getCancellationExceptionMessage());
      } else if (done) {
        if (resultCleared) {
          throw new IllegalStateException("Result cleared, future get's not possible");
        }
        return result;
      } else {
        throw new TimeoutException();
      }
    }
  }

  @Override
  public StackTraceElement[] getRunningStackTrace() {
    Thread t = runningThread;
    if (t == null) {
      return null;
    } else {
      StackTraceElement[] stack = t.getStackTrace();
      if (stack.length == 0 || t != runningThread) {
        return null;
      } else {
        return stack;
      }
    }
  }
}
