package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.util.ArgumentVerifier;

/**
 * A simple way to limit any {@link Executor} so that queues are managed.  In addition this queue 
 * is tracked completely independent of the {@link Executor}'s actual queue, so these can be 
 * distributed in code to limit queues differently to different parts of the system, while letting 
 * them all back the same {@link Executor}.
 * <p>
 * Once the limit has been reached, if additional tasks are supplied a 
 * {@link RejectedExecutionException} will be thrown.  This is the threadly equivalent of 
 * supplying a limited sized blocking queue to a java.util.concurrent thread pool.
 * <p>
 * See {@link SubmitterSchedulerQueueLimitRejector}, {@link SchedulerServiceQueueLimitRejector} 
 * and {@link PrioritySchedulerServiceQueueLimitRejector} as other possible implementations.
 *  
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class ExecutorQueueLimitRejector extends AbstractSubmitterExecutor {
  protected final Executor parentExecutor;
  protected final RejectedExecutionHandler rejectedExecutionHandler;
  protected final AtomicInteger queuedTaskCount;
  private volatile int queuedTaskLimit;
  
  /**
   * Constructs a new {@link ExecutorQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentExecutor Executor to execute tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public ExecutorQueueLimitRejector(Executor parentExecutor, int queuedTaskLimit) {
    this(parentExecutor, queuedTaskLimit, null);
  }
  
  /**
   * Constructs a new {@link ExecutorQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @since 4.8.0
   * @param parentExecutor Executor to execute tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed due to queue size
   */
  public ExecutorQueueLimitRejector(Executor parentExecutor, int queuedTaskLimit, 
                                    RejectedExecutionHandler rejectedExecutionHandler) {
    ArgumentVerifier.assertNotNull(parentExecutor, "parentExecutor");
    ArgumentVerifier.assertGreaterThanZero(queuedTaskLimit, "queuedTaskLimit");
    
    this.parentExecutor = parentExecutor;
    if (rejectedExecutionHandler == null) {
      rejectedExecutionHandler = RejectedExecutionHandler.THROW_REJECTED_EXECUTION_EXCEPTION;
    }
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    this.queuedTaskCount = new AtomicInteger();
    this.queuedTaskLimit = queuedTaskLimit;
  }
  
  /**
   * Invoked to check how many tasks are currently being tracked as queued by this limiter.
   * 
   * @return Number of tracked tasks waiting for execution to start
   */
  public int getQueuedTaskCount() {
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
    while (true) {
      int casValue = queuedTaskCount.get();
      if (casValue >= queuedTaskLimit) {
        rejectedExecutionHandler.handleRejectedTask(task);
        return; // in case handler did not throw exception
      } else if (queuedTaskCount.compareAndSet(casValue, casValue + 1)) {
        try {
          parentExecutor.execute(new DecrementingRunnable(task, queuedTaskCount));
        } catch (RejectedExecutionException e) {
          queuedTaskCount.decrementAndGet();
          throw e;
        }
        break;
      } // else loop and retry
    }
  }
  
  /**
   * This runnable decrements a provided AtomicInteger at the START of execution.
   * 
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
