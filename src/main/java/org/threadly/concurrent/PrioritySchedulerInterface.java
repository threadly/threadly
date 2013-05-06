package org.threadly.concurrent;

public interface PrioritySchedulerInterface extends SimpleSchedulerInterface {
  public void execute(Runnable task, TaskPriority priority);
  
  public void schedule(Runnable task, long delayInMs, TaskPriority priority);
  
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority);
}
