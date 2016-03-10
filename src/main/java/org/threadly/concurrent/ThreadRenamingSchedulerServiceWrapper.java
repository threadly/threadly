package org.threadly.concurrent;

import java.util.concurrent.Callable;

/**
 * <p>Class which wraps a {@link SchedulerService} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnableWrapper}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.</p>
 * 
 * @author jent
 * @since 4.3.0
 */
public class ThreadRenamingSchedulerServiceWrapper extends ThreadRenamingSubmitterSchedulerWrapper 
                                                   implements SchedulerService {
  protected final SchedulerService scheduler;
  
  /**
   * Constructs a new {@link ThreadRenamingSchedulerServiceWrapper}, wrapping a supplied 
   * {@link SchedulerService}.  If /{@code replace} is {@code false} the thread will be named such 
   * that {@code threadName[originalThreadName]}.
   * 
   * @param scheduler SchedulerService to wrap and send executions to
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingSchedulerServiceWrapper(SchedulerService scheduler, 
                                               String threadName, boolean replace) {
    super(scheduler, threadName, replace);
    
    this.scheduler = scheduler;
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
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  /**
   * Call to check how many tasks are currently being executed in this scheduler.
   * 
   * @deprecated Please use the better named {@link #getActiveTaskCount()}
   * 
   * @return current number of running tasks
   */
  @Override
  @Deprecated
  public int getCurrentRunningCount() {
    return scheduler.getCurrentRunningCount();
  }
  
  @Override
  public int getQueuedTaskCount() {
    return scheduler.getQueuedTaskCount();
  }

  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.
   * 
   * @deprecated Please use {@link #getQueuedTaskCount()} as a direct replacement.
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  @Override
  @Deprecated
  public int getScheduledTaskCount() {
    return scheduler.getScheduledTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}