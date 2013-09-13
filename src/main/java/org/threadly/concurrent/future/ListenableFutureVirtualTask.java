package org.threadly.concurrent.future;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.concurrent.VirtualCallable;
import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * A slightly more heavy weight implementation of {@link ListenableFutureTask} which can handle 
 * {@link VirtualCallable} and {@link VirtualRunnable} testable architectures.  The performance 
 * hit from this implementation is extremely minor (and can probably be improved in the future).  
 * But with that said, it should only be used if there is the possibility of needing to support 
 * a {@link VirtualCallable} or {@link VirtualRunnable} inside a TestableScheduler.  Otherwise 
 * the {@link ListenableFutureTask} is almost always a better choice.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of future implementation
 */
public class ListenableFutureVirtualTask<T> extends VirtualRunnable 
                                            implements ListenableRunnableFuture<T> {
  protected final Callable<T> callable;
  protected final VirtualLock lock;
  protected final Map<Runnable, Executor> listeners;
  protected boolean canceled;
  protected boolean started;
  protected boolean done;
  protected Throwable failure;
  protected T result;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   * @param lock lock to be used internally
   */
  public ListenableFutureVirtualTask(Runnable task, T result, VirtualLock lock) {
    this(VirtualCallable.fromRunnable(task, result), lock);
  }
  
  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   * @param lock lock to be used internally
   */
  public ListenableFutureVirtualTask(Callable<T> task, VirtualLock lock) {
    this.callable = task;
    this.lock = lock;
    this.listeners = new HashMap<Runnable, Executor>();
    this.canceled = false;
    this.started = false;
    this.done = false;
    this.failure = null;
    this.result = null;
  }
  
  private void callListeners() {
    synchronized (lock) {
      Iterator<Entry<Runnable, Executor>> it = listeners.entrySet().iterator();
      while (it.hasNext()) {
        Entry<Runnable, Executor> listener = it.next();
        runListener(listener.getKey(), listener.getValue(), false);
      }
      
      listeners.clear();
    }
  }
  
  private void runListener(Runnable listener, Executor executor, 
                           boolean throwException) {
    if (executor != null) {
      executor.execute(listener);
    } else {
      try {
        listener.run();
      } catch (RuntimeException e) {
        if (throwException) {
          throw e;
        } else {
          UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();
          if (handler != null) {
            handler.uncaughtException(Thread.currentThread(), e);
          } else {
            e.printStackTrace();
          }
        }
      }
    }
  }
  
  @Override
  public void run() {
    try {
      boolean shouldRun = false;
      synchronized (lock) {
        if (! canceled) {
          started = true;
          shouldRun = true;
        }
      }
      
      if (shouldRun) {
        if (factory != null && callable instanceof VirtualCallable) {
          result = ((VirtualCallable<T>)callable).call(factory);
        } else {
          result = callable.call();
        }
      }
      
      synchronized (lock) {
        done = true;
      
        callListeners();
        
        lock.signalAll();
      }
    } catch (Throwable t) {
      synchronized (lock) {
        done = true;
        failure = t;
      
        callListeners();
      
        lock.signalAll();
      }
    }
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    synchronized (lock) {
      canceled = true;
      
      callListeners();
      
      lock.signalAll();
    
      return ! started;
    }
  }

  @Override
  public boolean isDone() {
    synchronized (lock) {
      return done || canceled;
    }
  }

  @Override
  public boolean isCancelled() {
    synchronized (lock) {
      return canceled && ! started;
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

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                   ExecutionException,
                                                   TimeoutException {
    long startTime = Clock.accurateTime();
    long timeoutInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
    synchronized (lock) {
      long waitTime = timeoutInMs - (Clock.accurateTime() - startTime);
      while (! done && waitTime > 0) {
        lock.await(waitTime);
        waitTime = timeoutInMs - (Clock.accurateTime() - startTime);
      }
      
      if (canceled || 
          (failure != null && 
            (failure == StaticCancellationException.instance() || failure instanceof CancellationException))) {
        throw StaticCancellationException.instance();
      } else if (failure != null) {
        throw new ExecutionException(failure);
      } else if (! done) {
        throw new TimeoutException();
      }
      
      return result;
    }
  }

  @Override
  public void addListener(Runnable listener) {
    addListener(listener, null);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    if (listener == null) {
      throw new IllegalArgumentException("Can not provide a null listener runnable");
    }
    
    synchronized (lock) {
      if (isDone()) {
        runListener(listener, executor, true);
      } else {
        listeners.put(listener, executor);
      }
    }
  }
}
