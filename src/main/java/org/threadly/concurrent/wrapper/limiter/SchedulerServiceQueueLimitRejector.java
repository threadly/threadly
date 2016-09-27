package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SchedulerService;

/**
 * <p>A simple way to limit any {@link SchedulerService} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link SchedulerService}'s actual 
 * queue, so these can be distributed in code to limit queues differently to different parts of the 
 * system, while letting them all back the same {@link SchedulerService}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread 
 * pool.</p>
 * 
 * <p>See {@link ExecutorQueueLimitRejector}, {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link PrioritySchedulerServiceQueueLimitRejector} as other possible implementations.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class SchedulerServiceQueueLimitRejector extends SubmitterSchedulerQueueLimitRejector 
                                                implements SchedulerService {
  protected final SchedulerService parentScheduler;

  /**
   * Constructs a new {@link SchedulerServiceQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public SchedulerServiceQueueLimitRejector(SchedulerService parentScheduler, int queuedTaskLimit) {
    this(parentScheduler, queuedTaskLimit, null);
  }

  /**
   * Constructs a new {@link SchedulerServiceQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @since 4.8.0
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed due to queue size
   */
  public SchedulerServiceQueueLimitRejector(SchedulerService parentScheduler, int queuedTaskLimit, 
                                            RejectedExecutionHandler rejectedExecutionHandler) {
    super(parentScheduler, queuedTaskLimit, rejectedExecutionHandler);
    
    this.parentScheduler = parentScheduler;
  }

  @Override
  public boolean remove(Runnable task) {
    if (parentScheduler.remove(task)) {
      queuedTaskCount.decrementAndGet();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean remove(Callable<?> task) {
    if (parentScheduler.remove(task)) {
      queuedTaskCount.decrementAndGet();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int getActiveTaskCount() {
    return parentScheduler.getActiveTaskCount();
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
    return parentScheduler.getCurrentRunningCount();
  }

  @Override
  public int getQueuedTaskCount() {
    // no reason to defer to a possibly more expensive queue count from the scheduler
    return queuedTaskCount.get();
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
    return getQueuedTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return parentScheduler.isShutdown();
  }
}
