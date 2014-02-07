package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;

/**
 * <p>This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor}
 * to use that implementation with the {@link SubmitterSchedulerInterface}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
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
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    }
    
    ListenableFutureTask<T> fTask = new ListenableFutureTask<T>(false, task, result);
    
    scheduler.execute(fTask);
    
    return fTask;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Callable can not be null");
    }
    
    ListenableFutureTask<T> fTask = new ListenableFutureTask<T>(false, task);
    
    scheduler.execute(fTask);
    
    return fTask;
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
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    ListenableFutureTask<T> fTask = new ListenableFutureTask<T>(false, task, result);
    
    scheduler.schedule(fTask, delayInMs, TimeUnit.MILLISECONDS);
    
    return fTask;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Callable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    ListenableFutureTask<T> fTask = new ListenableFutureTask<T>(false, task);
    
    scheduler.schedule(fTask, delayInMs, TimeUnit.MILLISECONDS);
    
    return fTask;
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
}
