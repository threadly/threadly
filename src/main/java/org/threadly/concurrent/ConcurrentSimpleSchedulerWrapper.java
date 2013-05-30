package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper for the java.util.concurrent.ScheduledThreadPoolExecutor
 * to use that implementation with the SimpleSchedulerInterface.
 * 
 * @author jent - Mike Jensen
 */
public class ConcurrentSimpleSchedulerWrapper implements SimpleSchedulerInterface {
  private final ScheduledExecutorService scheduler;
  
  /**
   * Constructs a new wrapper with the provided scheduler implementation.
   * 
   * @param scheduler ScheduledExecutorService implementor
   */
  public ConcurrentSimpleSchedulerWrapper(ScheduledExecutorService scheduler) {
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
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    }
    
    return scheduler.submit(task);
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
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    return scheduler.schedule(task, delayInMs, 
                              TimeUnit.MILLISECONDS);
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
}
