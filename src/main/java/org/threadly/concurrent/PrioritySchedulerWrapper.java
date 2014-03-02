package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;

/**
 * <p>Class to wrap any implementation of {@link PrioritySchedulerInterface}.  The purpose of 
 * wrapping like this would be to change the default priority from the wrapped instance.  That 
 * way this could be passed into other parts of code and although use the same thread pool, 
 * have different default priorities.  (this could be particularly useful when used 
 * in combination with {@link TaskExecutorDistributor}, or {@link TaskSchedulerDistributor}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class PrioritySchedulerWrapper implements PrioritySchedulerInterface {
  protected final PrioritySchedulerInterface scheduler;
  protected final TaskPriority defaultPriority;
  
  /**
   * Constructs a new wrapper.
   * 
   * @param scheduler scheduler implementation to default to
   * @param defaultPriority default priority for tasks submitted without a priority
   */
  public PrioritySchedulerWrapper(PrioritySchedulerInterface scheduler, 
                                  TaskPriority defaultPriority) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide a scheduler");
    } else if (defaultPriority == null) {
      throw new IllegalArgumentException("Must provide a default priority");
    }
    
    this.scheduler = scheduler;
    this.defaultPriority = defaultPriority;
  }

  @Override
  public void execute(Runnable command) {
    scheduler.execute(command, defaultPriority);
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    scheduler.execute(task, priority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return scheduler.submit(task, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return scheduler.submit(task, result, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return scheduler.submit(task, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, 
                                        TaskPriority priority) {
    return scheduler.submit(task, result, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return scheduler.submit(task, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return scheduler.submit(task, priority);
  }
  
  @Override
  public void schedule(Runnable task, long delayInMs) {
    scheduler.schedule(task, delayInMs, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    scheduler.schedule(task, delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return scheduler.submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return scheduler.submitScheduled(task, result, delayInMs, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs,
                                             TaskPriority priority) {
    return scheduler.submitScheduled(task, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return scheduler.submitScheduled(task, result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return scheduler.submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return scheduler.submitScheduled(task, delayInMs, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                                     defaultPriority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay, 
                                     TaskPriority priority) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }
}