package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SchedulerService;

/**
 * A simple way to limit any {@link SchedulerService} so that queues are managed.  In addition 
 * this queue is tracked completely independent of the {@link SchedulerService}'s actual queue, so 
 * these can be distributed in code to limit queues differently to different parts of the system, 
 * while letting them all back the same {@link SchedulerService}.
 * <p>
 * Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread 
 * pool.
 * <p>
 * See {@link ExecutorQueueLimitRejector}, {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link PrioritySchedulerServiceQueueLimitRejector} as other possible implementations.
 *  
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
  public int getWaitingForExecutionTaskCount() {
    return parentScheduler.getWaitingForExecutionTaskCount();
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

  @Override
  public boolean isShutdown() {
    return parentScheduler.isShutdown();
  }
}
