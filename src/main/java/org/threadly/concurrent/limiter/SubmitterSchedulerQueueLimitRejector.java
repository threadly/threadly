package org.threadly.concurrent.limiter;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>A simple way to limit any {@link SubmitterScheduler} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link SubmitterScheduler}'s 
 * actual queue, so these can be distributed in code to limit queues differently to different 
 * parts of the system, while letting them all back the same {@link SubmitterScheduler}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link RejectedExecutionException} will be thrown.  This is the threadly equivalent of 
 * supplying a limited sized blocking queue to a java.util.concurrent thread pool.</p>
 * 
 * <p>See {@link ExecutorQueueLimitRejector} and {@link SchedulerServiceQueueLimitRejector} as 
 * other possible implementations.</p>
 * 
 * @deprecated replaced by version in {@link org.threadly.concurrent.wrapper.limiter}
 *  
 * @author jent
 * @since 4.3.0
 */
@Deprecated
public class SubmitterSchedulerQueueLimitRejector extends AbstractSubmitterScheduler {
  protected final SubmitterScheduler parentScheduler;
  protected final AtomicInteger queuedTaskCount;
  private int queuedTaskLimit;

  /**
   * Constructs a new {@link SubmitterSchedulerQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public SubmitterSchedulerQueueLimitRejector(SubmitterScheduler parentScheduler, int queuedTaskLimit) {
    ArgumentVerifier.assertNotNull(parentScheduler, "parentExecutor");
    
    this.parentScheduler = parentScheduler;
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
    if (queuedTaskCount.get() >= queuedTaskLimit) {
      throw new RejectedExecutionException();
    } else if (queuedTaskCount.incrementAndGet() > queuedTaskLimit) {
      queuedTaskCount.decrementAndGet();
      throw new RejectedExecutionException();
    } else {
      try {
        parentScheduler.schedule(new DecrementingRunnable(task, queuedTaskCount), delayInMillis);
      } catch (RejectedExecutionException e) {
        queuedTaskCount.decrementAndGet();
        throw e;
      }
    }
  }
  
  /**
   * <p>This runnable decrements a provided AtomicInteger at the START of execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected static class DecrementingRunnable implements Runnable, RunnableContainer {
    private final Runnable task;
    private final AtomicInteger queuedTaskCount;
    
    public DecrementingRunnable(Runnable task, AtomicInteger queuedTaskCount) {
      this.task = task;
      this.queuedTaskCount = queuedTaskCount;
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }

    @Override
    public void run() {
      queuedTaskCount.decrementAndGet();
      task.run();
    }
  }
}
