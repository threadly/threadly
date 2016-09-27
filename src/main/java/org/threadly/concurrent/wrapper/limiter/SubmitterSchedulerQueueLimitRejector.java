package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.wrapper.limiter.ExecutorQueueLimitRejector.DecrementingRunnable;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>A simple way to limit any {@link SubmitterScheduler} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link SubmitterScheduler}'s 
 * actual queue, so these can be distributed in code to limit queues differently to different 
 * parts of the system, while letting them all back the same {@link SubmitterScheduler}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied the rejected execution 
 * handler will be invoked with the rejected tasks (which by default will throw a 
 * {@link RejectedExecutionException}).  This is the threadly equivalent of supplying a limited 
 * sized blocking queue to a java.util.concurrent thread pool.</p>
 * 
 * <p>See {@link ExecutorQueueLimitRejector}, {@link SchedulerServiceQueueLimitRejector} and 
 * {@link PrioritySchedulerServiceQueueLimitRejector} as other possible implementations.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class SubmitterSchedulerQueueLimitRejector extends AbstractSubmitterScheduler {
  protected final SubmitterScheduler parentScheduler;
  protected final RejectedExecutionHandler rejectedExecutionHandler;
  protected final AtomicInteger queuedTaskCount;
  private int queuedTaskLimit;

  /**
   * Constructs a new {@link SubmitterSchedulerQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public SubmitterSchedulerQueueLimitRejector(SubmitterScheduler parentScheduler, int queuedTaskLimit) {
    this(parentScheduler, queuedTaskLimit, null);
  }

  /**
   * Constructs a new {@link SubmitterSchedulerQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @since 4.8.0
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed due to queue size
   */
  public SubmitterSchedulerQueueLimitRejector(SubmitterScheduler parentScheduler, int queuedTaskLimit, 
                                              RejectedExecutionHandler rejectedExecutionHandler) {
    ArgumentVerifier.assertNotNull(parentScheduler, "parentExecutor");
    
    this.parentScheduler = parentScheduler;
    if (rejectedExecutionHandler == null) {
      rejectedExecutionHandler = RejectedExecutionHandler.THROW_REJECTED_EXECUTION_EXCEPTION;
    }
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    this.queuedTaskCount = new AtomicInteger();
    this.queuedTaskLimit = queuedTaskLimit;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    // we don't track recurring tasks
    parentScheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    // we don't track recurring tasks
    parentScheduler.scheduleAtFixedRate(task, initialDelay, period);
  }
  
  /**
   * Invoked to check how many tasks are currently being tracked as queued by this limiter.
   * 
   * @return Number of tracked tasks waiting for execution to start
   */
  public int getCurrentQueueSize() {
    return queuedTaskCount.get();
  }
  
  /**
   * Invoked to check the currently set queue limit.
   * 
   * @return Maximum number of tasks allowed to queue in the parent executor
   */
  public int getQueueLimit() {
    return queuedTaskLimit;
  }
  
  /**
   * Invoked to change the set limit.  Negative and zero limits are allowed, but they will cause 
   * all executions to be rejected.  If set below the current queue size, those tasks will still 
   * remain queued for execution.
   * 
   * @param newLimit New limit to avoid executions 
   */
  public void setQueueLimit(int newLimit) {
    this.queuedTaskLimit = newLimit;
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    while (true) {
      int casValue = queuedTaskCount.get();
      if (casValue >= queuedTaskLimit) {
        rejectedExecutionHandler.handleRejectedTask(task);
        return; // in case handler did not throw exception
      } else if (queuedTaskCount.compareAndSet(casValue, casValue + 1)) {
        try {
          parentScheduler.schedule(new DecrementingRunnable(task, queuedTaskCount), delayInMillis);
        } catch (RejectedExecutionException e) {
          queuedTaskCount.decrementAndGet();
          throw e;
        }
        break;
      } // else loop and retry
    }
  }
}
