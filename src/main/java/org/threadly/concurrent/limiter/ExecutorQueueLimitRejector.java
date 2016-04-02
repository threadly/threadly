package org.threadly.concurrent.limiter;

import java.util.concurrent.Executor;

/**
 * <p>A simple way to limit any {@link Executor} so that queues are managed.  In addition this 
 * queue is tracked completely independent of the {@link Executor}'s actual queue, so these can be 
 * distributed in code to limit queues differently to different parts of the system, while letting 
 * them all back the same {@link Executor}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread pool.</p>
 * 
 * <p>See {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link SchedulerServiceQueueLimitRejector} as other possible implementations.</p>
 * 
 * @deprecated moved to {@link org.threadly.concurrent.wrapper.limiter.ExecutorQueueLimitRejector}
 *  
 * @author jent
 * @since 4.3.0
 */
@Deprecated
public class ExecutorQueueLimitRejector extends org.threadly.concurrent.wrapper.limiter.ExecutorQueueLimitRejector {
  /**
   * Constructs a new {@link ExecutorQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentExecutor Executor to execute tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public ExecutorQueueLimitRejector(Executor parentExecutor, int queuedTaskLimit) {
    super(parentExecutor, queuedTaskLimit);
  }
  
  /**
   * Invoked to check how many tasks are currently being tracked as queued by this limiter.
   * 
   * @deprecated Please use {@link #getQueuedTaskCount()} as a direct replacement.
   * 
   * @return Number of tracked tasks waiting for execution to start
   */
  @Deprecated
  public int getCurrentQueueSize() {
    return queuedTaskCount.get();
  }
}
