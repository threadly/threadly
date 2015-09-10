package org.threadly.concurrent.limiter;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractRunnableContainer;
import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>A simple way to limit any {@link Executor} so that queues are managed.  In addition this 
 * queue is tracked completely independent of the {@link Executor}'s actual queue, so these can be 
 * distributed in code to limit queues differently to different parts of the system, while letting 
 * them all back the same {@link Executor}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link RejectedExecutionException} will be thrown.  This is the threadly equivalent of 
 * supplying a limited sized blocking queue to a java.util.concurrent thread pool.</p>
 * 
 * <p>See {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link SchedulerServiceQueueLimitRejector} as other possible implementations.</p>
 *  
 * @author jent
 * @since 4.3.0
 */
public class ExecutorQueueLimitRejector extends AbstractSubmitterExecutor {
  protected final Executor parentExecutor;
  protected final AtomicInteger queuedTaskCount;
  private int queuedTaskLimit;
  
  /**
   * Constructs a new {@link ExecutorQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentExecutor Executor to execute tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public ExecutorQueueLimitRejector(Executor parentExecutor, int queuedTaskLimit) {
    ArgumentVerifier.assertNotNull(parentExecutor, "parentExecutor");
    
    this.parentExecutor = parentExecutor;
    this.queuedTaskCount = new AtomicInteger();
    this.queuedTaskLimit = queuedTaskLimit;
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
  protected void doExecute(Runnable task) {
    if (queuedTaskCount.get() >= queuedTaskLimit) {
      throw new RejectedExecutionException();
    } else if (queuedTaskCount.incrementAndGet() > queuedTaskLimit) {
      queuedTaskCount.decrementAndGet();
      throw new RejectedExecutionException();
    } else {
      parentExecutor.execute(new DecrementingRunnable(task, queuedTaskCount));
    }
  }
  
  /**
   * <p>This runnable decrements a provided AtomicInteger at the START of execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected static class DecrementingRunnable extends AbstractRunnableContainer implements Runnable {
    private final AtomicInteger queuedTaskCount;
    
    public DecrementingRunnable(Runnable task, AtomicInteger queuedTaskCount) {
      super(task);
      
      this.queuedTaskCount = queuedTaskCount;
    }

    @Override
    public void run() {
      queuedTaskCount.decrementAndGet();
      runnable.run();
    }
  }
}
