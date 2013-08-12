package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor}
 * to use that implementation with the {@link SubmitterSchedulerInterface}.
 * 
 * @author jent - Mike Jensen
 */
public class ScheduledExecutorServiceWrapper implements SubmitterSchedulerInterface {
  private final ScheduledExecutorService scheduler;
  
  /**
   * Constructs a new wrapper with the provided scheduler implementation.
   * 
   * @param scheduler ScheduledExecutorService implementor
   */
  public ScheduledExecutorServiceWrapper(ScheduledExecutorService scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler implementation");
    }
    
    this.scheduler = scheduler;
  }
  
  @Override
  public void execute(Runnable command) {
    if (command == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    }
    
    scheduler.execute(command);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    }
    
    return scheduler.submit(task, result);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Callable can not be null");
    }
    
    return scheduler.submit(task);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    scheduler.schedule(task, delayInMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> Future<T> submitScheduled(Runnable task, T result, 
                                       long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    Future<?> future = scheduler.schedule(task, delayInMs, 
                                          TimeUnit.MILLISECONDS);
    
    return new FutureWrapper<T>(future, result);
  }

  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Callable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    return scheduler.schedule(task, delayInMs, 
                              TimeUnit.MILLISECONDS);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                                     TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
  
  /**
   * This is a wrapper to change the result for a given future.  It 
   * can take a future with type <?> and can return any result desired.
   * 
   * @author jent - Mike Jensen
   * @param <T> type for futures result
   */
  private static class FutureWrapper<T> implements Future<T> {
    private final Future<?> future;
    private final T result;
    
    private FutureWrapper(Future<?> future, T result) {
      this.future = future;
      this.result = result;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return future.isCancelled();
    }

    @Override
    public boolean isDone() {
      return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      future.get(); // blocks
      
      return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, 
                                                     ExecutionException, 
                                                     TimeoutException {
      future.get(timeout, unit);  // blocks
      
      return result;
    }
  }
}
