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
public class ThreadRenamingSchedulerService extends ThreadRenamingSubmitterScheduler 
                                            implements SchedulerService {
  protected final SchedulerService scheduler;
  
  /**
   * Constructs a new {@link ThreadRenamingSchedulerService}, wrapping a supplied 
   * {@link SchedulerService}.  If /{@code replace} is {@code false} the thread will be named such 
   * that {@code threadName[originalThreadName]}.
   * 
   * @param scheduler SchedulerService to wrap and send executions to
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingSchedulerService(SchedulerService scheduler, String threadName, boolean replace) {
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
  public int getCurrentRunningCount() {
    return scheduler.getCurrentRunningCount();
  }

  @Override
  public int getScheduledTaskCount() {
    return scheduler.getScheduledTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}