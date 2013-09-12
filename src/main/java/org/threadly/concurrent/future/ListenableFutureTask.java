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
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is a future which can be executed.  Allowing you to construct the future with 
 * the interior work, submit it to an executor, and then return this future.
 * 
 * @author jent - Mike Jensen
 * @param <T> type of future implementation
 */
public class ListenableFutureTask<T> extends FutureTask<T> 
                                     implements ListenableRunnableFuture<T> {
  protected final boolean recurring;
  protected final Map<Runnable, Executor> listeners;
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   */
  public ListenableFutureTask(boolean recurring, Runnable task) {
    this(recurring, task, null);
  }
  
  /**
   * Constructs a runnable future with a runnable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task runnable to be run
   * @param result result to be provide after run has completed
   */
  public ListenableFutureTask(boolean recurring, Runnable task, T result) {
    this(recurring, Executors.callable(task, result));
  }

  /**
   * Constructs a runnable future with a callable work unit.
   * 
   * @param recurring boolean to indicate if this task can run multiple times, and thus must be reset after each run
   * @param task callable to be run
   */
  public ListenableFutureTask(boolean recurring, Callable<T> task) {
    super(task);
    
    this.recurring = recurring;
    this.listeners = new HashMap<Runnable, Executor>();
  }
  
  @Override
  public void run() {
    if (recurring) {
      super.runAndReset();
    } else {
      super.run();
    }
  }
  
  private void callListeners() {
    synchronized (listeners) {
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
  public void addListener(Runnable listener) {
    addListener(listener, null);
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    if (listener == null) {
      throw new IllegalArgumentException("Can not provide a null listener runnable");
    }
    
    synchronized (listeners) {
      if (isDone()) {
        runListener(listener, executor, true);
      } else {
        listeners.put(listener, executor);
      }
    }
  }
  
  @Override
  protected void done() {
    callListeners();
  }
  
  @Override
  public T get() throws InterruptedException, 
                        ExecutionException {
    try {
      return super.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause == StaticCancellationException.instance() || 
          cause instanceof CancellationException) {
        throw StaticCancellationException.instance();
      } else {
        throw e;
      }
    }
  }
  
  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    try {
      return super.get(timeout, unit);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause == StaticCancellationException.instance() || 
          cause instanceof CancellationException) {
        throw StaticCancellationException.instance();
      } else {
        throw e;
      }
    }
  }
}