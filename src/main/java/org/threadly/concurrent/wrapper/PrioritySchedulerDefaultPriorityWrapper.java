package org.threadly.concurrent.wrapper;

import java.util.concurrent.Callable;

import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * Class to wrap any implementation of {@link PrioritySchedulerService}.  The purpose of wrapping 
 * like this would be to change the default priority from the wrapped instance.  That way this 
 * could be passed into other parts of code and although use the same thread pool, have different 
 * default priorities.  (this could be particularly useful when used in combination with 
 * {@link KeyDistributedExecutor}, or {@link KeyDistributedScheduler}.
 * 
 * @since 4.6.0 (since 1.0.0 as org.threadly.concurrent.PrioritySchedulerWrapper)
 */
public class PrioritySchedulerDefaultPriorityWrapper implements PrioritySchedulerService {
  /**
   * Convenience function for wrapping the scheduler if the default priority is not what is desired.  
   * If it is already the desired priority it will simply return the provided reference directly.
   * 
   * @since 5.8
   * @param scheduler Scheduler to check priority against or wrap
   * @param defaultPriority The default priority the returned scheduler should have
   * @return A scheduler with the set default task priority
   */
  public static PrioritySchedulerService wrapIfNecessary(PrioritySchedulerService scheduler, 
                                                         TaskPriority defaultPriority) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    ArgumentVerifier.assertNotNull(defaultPriority, "defaultPriority");
    
    if (scheduler.getDefaultPriority() == defaultPriority) {
      return scheduler;
    } else {
      return new PrioritySchedulerDefaultPriorityWrapper(scheduler, defaultPriority);
    }
  }
  
  protected final PrioritySchedulerService scheduler;
  protected final TaskPriority defaultPriority;
  
  /**
   * Constructs a new priority wrapper with a new default priority to use.
   * 
   * @param scheduler PriorityScheduler implementation to default to
   * @param defaultPriority default priority for tasks submitted without a priority
   */
  public PrioritySchedulerDefaultPriorityWrapper(PrioritySchedulerService scheduler, 
                                                 TaskPriority defaultPriority) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    ArgumentVerifier.assertNotNull(defaultPriority, "defaultPriority");
    
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
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
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
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
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
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, defaultPriority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay, 
                                     TaskPriority priority) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduler.scheduleAtFixedRate(task, initialDelay, period, defaultPriority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    scheduler.scheduleAtFixedRate(task, initialDelay, period, priority);
  }

  @Override
  public boolean remove(Runnable task) {
    return scheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return scheduler.remove(task);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return scheduler.getMaxWaitForLowPriority();
  }

  @Override
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }
  
  @Override
  public int getQueuedTaskCount() {
    return scheduler.getQueuedTaskCount();
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    return scheduler.getQueuedTaskCount(priority);
  }

  @Override
  public int getWaitingForExecutionTaskCount() {
    return scheduler.getWaitingForExecutionTaskCount();
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    return scheduler.getWaitingForExecutionTaskCount(priority);
  }
}
