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
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * This is a future which can be executed.  Allowing you to construct the future with 
 * the interior work, submit it to an executor, and then return this future.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of future implementation
 */
public class RunnableFuture<T> extends VirtualRunnable 
                               implements ListenableFuture<T>, java.util.concurrent.RunnableFuture<T> {
  protected final Runnable runnable;
  protected final T runnableResult;
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
   */
  public RunnableFuture(Runnable task) {
    this(task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public RunnableFuture(Runnable task, T result) {
    this(task, result, new NativeLock());
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   * @param lock lock to be used internally
   */
  public RunnableFuture(Runnable task, T result, VirtualLock lock) {
    this.runnable = task;
    this.runnableResult = result;
    this.callable = null;
    this.lock = lock;
    this.listeners = new HashMap<Runnable, Executor>();
    this.canceled = false;
    this.started = false;
    this.done = false;
    this.failure = null;
    this.result = null;
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   */
  public RunnableFuture(Callable<T> task) {
    this(task, new NativeLock());
  }
  
  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param task callable to be run
   * @param lock lock to be used internally
   */
  public RunnableFuture(Callable<T> task, VirtualLock lock) {
    this.runnable = null;
    this.runnableResult = null;
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
        if (runnable != null) {
          if (factory != null && runnable instanceof VirtualRunnable) {
            ((VirtualRunnable)runnable).run(factory);
          } else {
            runnable.run();
          }
          result = runnableResult;
        } else {
          if (factory != null && callable instanceof VirtualCallable) {
            result = ((VirtualCallable<T>)callable).call(factory);
          } else {
            result = callable.call();
          }
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
      
      throw ExceptionUtils.makeRuntime(t);
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
      return done;
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
      
      if (canceled) {
        throw new CancellationException();
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
    synchronized (lock) {
      if (done || canceled) {
        runListener(listener, executor, true);
      } else {
        listeners.put(listener, executor);
      }
    }
  }
}