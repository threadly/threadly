package org.threadly.concurrent.wrapper.interceptor;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SchedulerService;

/**
 * <p>Class to wrap {@link SchedulerService} pool so that tasks can be intercepted and either 
 * wrapped, or modified, before being submitted to the pool.  This abstract class needs to have 
 * {@link #wrapTask(Runnable, boolean)} overridden to provide the task which should be submitted to 
 * the {@link SchedulerService}.  Please see the javadocs of {@link #wrapTask(Runnable, boolean)} 
 * for more details about ways a task can be modified or wrapped.</p>
 * 
 * <p>Other variants of task wrappers: {@link ExecutorTaskInterceptor}, 
 * {@link SubmitterSchedulerTaskInterceptor}, {@link PrioritySchedulerTaskInterceptor}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0
 */
public abstract class SchedulerServiceTaskInterceptor extends SubmitterSchedulerTaskInterceptor 
                                                      implements SchedulerService {
  protected final SchedulerService parentScheduler;
  
  protected SchedulerServiceTaskInterceptor(SchedulerService parentScheduler) {
    super(parentScheduler);
    
    this.parentScheduler = parentScheduler;
  }

  @Override
  public boolean remove(Runnable task) {
    return parentScheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return parentScheduler.remove(task);
  }

  @Override
  public int getActiveTaskCount() {
    return parentScheduler.getActiveTaskCount();
  }

  @Override
  public int getQueuedTaskCount() {
    return parentScheduler.getQueuedTaskCount();
  }

  /**
   * Call to check how many tasks are currently being executed in this scheduler.
   * 
   * @deprecated Please use the better named {@link #getActiveTaskCount()} as a direct replacement.
   * 
   * @return current number of running tasks
   */
  @Override
  @Deprecated
  public int getCurrentRunningCount() {
    return parentScheduler.getCurrentRunningCount();
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
    return parentScheduler.getScheduledTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return parentScheduler.isShutdown();
  }
}
