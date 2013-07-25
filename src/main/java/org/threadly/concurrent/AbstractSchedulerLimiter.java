package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * Abstract limiter for any implementations which need to schedule and handle futures.
 * 
 * @author jent - Mike Jensen
 */
public abstract class AbstractSchedulerLimiter extends AbstractThreadPoolLimiter {
  /**
   * Constructor for abstract class to call into for anyone extending this class.
   * 
   * @param subPoolName name to give threads while tasks running in pool (null to not change thread names)
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  protected AbstractSchedulerLimiter(String subPoolName, int maxConcurrency) {
    super(subPoolName, maxConcurrency);
  }
  
  protected class RunnableWrapper extends VirtualRunnable {
    private final Runnable runnable;
    
    public RunnableWrapper(Runnable runnable) {
      this.runnable = runnable;
    }
    
    @Override
    public void run() {
      Thread currentThread = null;
      String originalThreadName = null;
      if (subPoolName != null) {
        currentThread = Thread.currentThread();
        originalThreadName = currentThread.getName();
        
        currentThread.setName(makeSubPoolThreadName(originalThreadName));
      }
      
      try {
        if (factory != null && 
            runnable instanceof VirtualRunnable) {
          VirtualRunnable vr = (VirtualRunnable)runnable;
          vr.run(factory);
        } else {
          runnable.run();
        }
      } finally {
        handleTaskFinished();
          
        if (subPoolName != null) {
          currentThread.setName(originalThreadName);
        }
      }
    }
  }
  
  /**
   * ListenableFuture which contains a parent {@link Future}. 
   * (which may not be created yet).
   * 
   * @author jent - Mike Jensen
   * @param <T> result type returned by .get()
   */
  protected static class FutureFuture<T> implements Future<T> {
    private boolean canceled;
    private boolean mayInterruptIfRunningOnCancel;
    private Future<?> parentFuture;
    
    public FutureFuture() {
      canceled = false;
      parentFuture = null;
    }
    
    protected void setParentFuture(Future<?> parentFuture) {
      synchronized (this) {
        this.parentFuture = parentFuture;
        if (canceled) {
          parentFuture.cancel(mayInterruptIfRunningOnCancel);
        }
        
        this.notifyAll();
      }
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this) {
        canceled = true;
        mayInterruptIfRunningOnCancel = mayInterruptIfRunning;
        if (parentFuture != null) {
          return parentFuture.cancel(mayInterruptIfRunning);
        } else {
          return true;  // this is not guaranteed to be true, but is likely
        }
      }
    }

    @Override
    public boolean isCancelled() {
      synchronized (this) {
        return canceled;
      }
    }

    @Override
    public boolean isDone() {
      synchronized (this) {
        if (parentFuture == null) {
          return false;
        } else {
          return parentFuture.isDone();
        }
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
  }
}
