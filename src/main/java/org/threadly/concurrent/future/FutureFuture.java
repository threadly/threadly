package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * Future implementation which contains a parent {@link Future}.
 * This allows a future to be returned before an executor has actually produced a future 
 * which this class will rely on.
 * 
 * @author jent - Mike Jensen
 * @param <T> result type returned by .get()
 */
public class FutureFuture<T> implements Future<T> {
  private volatile TaskCanceler canceler;
  private volatile boolean canceled;
  private volatile Future<?> parentFuture;

  /**
   * Constructs a new Future instance which will 
   * depend on a Future instance to be provided later.
   */
  public FutureFuture() {
    this.canceler = null;
    canceled = false;
    parentFuture = null;
  }
  
  /**
   * Call to set a canceler for the task which this future represents.  If 
   * this future gets a call to cancel before the parent future is set, 
   * we will attempt to call to this canceler to cancel the task before it runs.
   * 
   * @param canceler canceler to call if parent future is not set
   */
  public void setTaskCanceler(TaskCanceler canceler) {
    if (this.canceler != null) {
      throw new IllegalStateException("Canceler has already been set");
    } else if (canceler == null) {
      throw new IllegalArgumentException("Must provide a non-null canceler");
    }
    
    this.canceler = canceler;
  }
  
  /**
   * Once the parent Future is available, this function should 
   * be called to provide it.
   * 
   * @param parentFuture ListenableFuture instance to depend on.
   */
  public void setParentFuture(Future<?> parentFuture) {
    if (this.parentFuture != null) {
      throw new IllegalStateException("Parent future has already been set");
    } else if (parentFuture == null) {
      throw new IllegalArgumentException("Must provide a non-null parent future");
    }
    
    this.parentFuture = parentFuture;
    synchronized (this) {
      this.notifyAll();
    }
  }
  
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (canceled) {
      return true;
    }
    if (parentFuture != null) {
      return canceled = parentFuture.cancel(mayInterruptIfRunning);
    } else if (canceler != null) {
      return canceled = canceler.cancel();
    } else {
      return false;
    }
  }

  @Override
  public boolean isCancelled() {
    return canceled;
  }

  @Override
  public boolean isDone() {
    if (parentFuture == null) {
      return false;
    } else {
      return parentFuture.isDone();
    }
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    try {
      return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // basically impossible
      throw ExceptionUtils.makeRuntime(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                   ExecutionException,
                                                   TimeoutException {
    long startTime = Clock.accurateTime();
    long timeoutInMillis = TimeUnit.MILLISECONDS.convert(timeout, unit);
    synchronized (this) {
      long remainingWaitTime = timeoutInMillis;
      while (parentFuture == null && remainingWaitTime > 0) {
        this.wait(remainingWaitTime);
        remainingWaitTime = timeoutInMillis - (Clock.accurateTime() - startTime);
      }
      if (remainingWaitTime <= 0) {
        throw new TimeoutException();
      }
      // parent future is now not null
      return (T)parentFuture.get(remainingWaitTime, TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * A canceler which this future will use if a parent future has not been set.
   * 
   * @author jent - Mike Jensen
   */
  public interface TaskCanceler {
    /**
     * Attempt to cancel a task before it has run.
     * 
     * @return true if and only if the task can be canceled before it is run
     */
    public boolean cancel();
  }
}