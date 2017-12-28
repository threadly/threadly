package org.threadly.concurrent.future;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.threadly.concurrent.event.RunnableListenerHelper;
import org.threadly.util.Clock;

/**
 * This class is designed to be a helper when returning a single result asynchronously.  This is 
 * particularly useful if this result is produced over multiple threads (and thus the scheduler 
 * returned future is not useful).
 * 
 * @since 1.2.0
 * @param <T> The result object type returned by this future
 */
public class SettableListenableFuture<T> implements ListenableFuture<T>, FutureCallback<T> {
  protected final RunnableListenerHelper listenerHelper;
  protected final Object resultLock;
  protected final boolean throwIfAlreadyComplete;
  private volatile Thread runningThread;
  private volatile boolean done;
  private volatile boolean canceled;
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
    this.listenerHelper = new RunnableListenerHelper(true);
    this.resultLock = new Object();
    this.throwIfAlreadyComplete = throwIfAlreadyComplete;
    this.runningThread = null;
    done = false;
    resultCleared = false;
    result = null;
    failure = null;
  }

  @Override
  public void addListener(Runnable listener, Executor executor, boolean optimizeExecution) {
    listenerHelper.addListener(listener, executor);
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
   * {@link ListenableFuture} implementation.  
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
    setFailure(t);
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
    if (done) {
      return false;
    }
    
    boolean canceled;
    synchronized (resultLock) {
      canceled = ! done;
      if (canceled) {
        this.canceled = true;
        
        if (interruptThread) {
          Thread runningThread = this.runningThread;
          if (runningThread != null) {
            runningThread.interrupt();
          }
        }
        
        setDone(null);
      }
    }
    
    if (canceled) {
      // call outside of lock
      listenerHelper.callListeners();
    }
    
    return canceled;
  }

  @Override
  public boolean isCancelled() {
    return canceled;
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
  
  // should be synchronized on resultLock before calling
  private boolean setDone(Throwable cause) {
    if (done) {
      if (throwIfAlreadyComplete) {
        throw new IllegalStateException("Future already done", cause);
      }
      
      return false;
    }
    
    done = true;
    runningThread = null;
    
    resultLock.notifyAll();
    return true;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    synchronized (resultLock) {
      while (! done) {
        resultLock.wait();
      }
      if (resultCleared) {
        throw new IllegalStateException("Result cleared, future get's not possible");
      }
      
      if (failure != null) {
        throw new ExecutionException(failure);
      } else if (canceled) {
        throw new CancellationException();
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
      if (resultCleared) {
        throw new IllegalStateException("Result cleared, future get's not possible");
      }
      
      if (failure != null) {
        throw new ExecutionException(failure);
      } else if (canceled) {
        throw new CancellationException();
      } else if (done) {
        return result;
      } else {
        throw new TimeoutException();
      }
    }
  }
}
