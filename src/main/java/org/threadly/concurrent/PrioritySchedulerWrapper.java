package org.threadly.concurrent;

/**
 * Class to wrap any implementation of PrioritySchedulerInterface.  The purpose of wrapping 
 * like this would be to change the default priority from the wrapped instance.  That way 
 * this could be passed into other parts of code and although use the same thread pool, 
 * have different default priorities.  (this could be particularly useful when used 
 * in combination with {@link CallableDistributor}, {@link TaskExecutorDistributor}, or 
 * {@link TaskSchedulerDistributor}.
 * 
 * @author jent - Mike Jensen
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
    execute(command, defaultPriority);
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
  }

  @Override
  public ExecuteFuture submit(Runnable task) {
    return submit(task, defaultPriority);
  }

  @Override
  public ExecuteFuture submit(Runnable task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }
  
  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                                TaskPriority priority) {
    scheduler.schedule(task, delayInMs, priority);
  }

  @Override
  public ExecuteFuture submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public ExecuteFuture submitScheduled(Runnable task, long delayInMs,
                                       TaskPriority priority) {
    return scheduler.submitScheduled(task, delayInMs, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
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