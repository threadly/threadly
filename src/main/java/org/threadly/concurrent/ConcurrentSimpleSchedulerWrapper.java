package org.threadly.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is a wrapper for the java.util.concurrent.ScheduledThreadPoolExecutor
 * to use that implementation with the SimpleSchedulerInterface.
 * 
 * @author jent - Mike Jensen
 */
public class ConcurrentSimpleSchedulerWrapper implements SimpleSchedulerInterface, 
                                                         ScheduledExecutorService {
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
  public void schedule(Runnable task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    scheduler.schedule(task, delayInMs, TimeUnit.MILLISECONDS);
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

  @Override
  public void shutdown() {
    scheduler.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return scheduler.shutdownNow();
  }

  @Override
  public boolean isTerminated() {
    return scheduler.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, 
                                  TimeUnit unit) throws InterruptedException {
    return scheduler.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return scheduler.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return scheduler.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
    return scheduler.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                       long timeout, TimeUnit unit) throws InterruptedException {
    return scheduler.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
                                                                         ExecutionException {
    return invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, 
                         long timeout, TimeUnit unit) throws InterruptedException,
                                                             ExecutionException, 
                                                             TimeoutException {
    return invokeAny(tasks, timeout, unit);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return scheduler.schedule(command, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, 
                                         long delay, TimeUnit unit) {
    return scheduler.schedule(callable, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
                                                long initialDelay, 
                                                long period,
                                                TimeUnit unit) {
    return scheduler.scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
                                                   long initialDelay,
                                                   long delay, 
                                                   TimeUnit unit) {
    return scheduler.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }
}
