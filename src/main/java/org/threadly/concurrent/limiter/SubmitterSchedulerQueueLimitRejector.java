package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SubmitterScheduler;

/**
 * <p>A simple way to limit any {@link SubmitterScheduler} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link SubmitterScheduler}'s 
 * actual queue, so these can be distributed in code to limit queues differently to different 
 * parts of the system, while letting them all back the same {@link SubmitterScheduler}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread pool.</p>
 * 
 * <p>See {@link ExecutorQueueLimitRejector} and {@link SchedulerServiceQueueLimitRejector} as 
 * other possible implementations.</p>
 * 
 * @deprecated Replaced by {@link org.threadly.concurrent.wrapper.limiter.SubmitterSchedulerQueueLimitRejector}
 *  
 * @author jent
 * @since 4.3.0
 */
@Deprecated
public class SubmitterSchedulerQueueLimitRejector 
       extends org.threadly.concurrent.wrapper.limiter.SubmitterSchedulerQueueLimitRejector {
  /**
   * Constructs a new {@link SubmitterSchedulerQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public SubmitterSchedulerQueueLimitRejector(SubmitterScheduler parentScheduler, int queuedTaskLimit) {
    super(parentScheduler, queuedTaskLimit);
  }
}
