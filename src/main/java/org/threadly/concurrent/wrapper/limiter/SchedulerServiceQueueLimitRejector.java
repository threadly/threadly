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
 * <p>See {@link ExecutorQueueLimitRejector} and {@link SubmitterSchedulerQueueLimitRejector} as 
 * other possible implementations.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.3.0
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
    super(parentScheduler, queuedTaskLimit);
    
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

  @Override
  public int getQueuedTaskCount() {
    // no reason to defer to a possibly more expensive queue count from the scheduler
    return queuedTaskCount.get();
  }

  @Override
  public boolean isShutdown() {
    return parentScheduler.isShutdown();
  }
}
