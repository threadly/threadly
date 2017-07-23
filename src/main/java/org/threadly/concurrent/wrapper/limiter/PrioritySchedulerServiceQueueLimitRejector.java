package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.wrapper.limiter.ExecutorQueueLimitRejector.DecrementingRunnable;
import org.threadly.util.ArgumentVerifier;

/**
 * A simple way to limit any {@link PrioritySchedulerService} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link PrioritySchedulerService}'s 
 * actual queue, so these can be distributed in code to limit queues differently to different parts 
 * of the system, while letting them all back the same {@link PrioritySchedulerService}.
 * <p>
 * Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread 
 * pool.
 * <p>
 * See {@link ExecutorQueueLimitRejector}, {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link SchedulerServiceQueueLimitRejector} as other possible implementations.
 *  
 * @since 4.8.0
 */
public class PrioritySchedulerServiceQueueLimitRejector extends SchedulerServiceQueueLimitRejector 
                                                        implements PrioritySchedulerService {
  protected final PrioritySchedulerService parentScheduler;
  protected final boolean dontLimitStarvable;

  /**
   * Constructs a new {@link PrioritySchedulerServiceQueueLimitRejector} with the provided 
   * scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public PrioritySchedulerServiceQueueLimitRejector(PrioritySchedulerService parentScheduler, int queuedTaskLimit) {
    this(parentScheduler, queuedTaskLimit, null);
  }

  /**
   * Constructs a new {@link PrioritySchedulerServiceQueueLimitRejector} with the provided 
   * scheduler, limit, and handler for when task can not be submitted to the pool.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed due to queue size
   */
  public PrioritySchedulerServiceQueueLimitRejector(PrioritySchedulerService parentScheduler, int queuedTaskLimit, 
                                                    RejectedExecutionHandler rejectedExecutionHandler) {
    this(parentScheduler, queuedTaskLimit, false, rejectedExecutionHandler);
  }

  /**
   * Constructs a new {@link PrioritySchedulerServiceQueueLimitRejector} with the provided 
   * scheduler and limit.  This constructor additionally allows you to specify if starvable tasks 
   * should be included in the queue limit.  Because starvable tasks have less impact on the pool, 
   * the need to limit them may be reduced.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param dontLimitStarvable Provide {@code true} to don't include starvable tasks against queue limit
   */
  public PrioritySchedulerServiceQueueLimitRejector(PrioritySchedulerService parentScheduler, 
                                                    int queuedTaskLimit, boolean dontLimitStarvable) {
    this(parentScheduler, queuedTaskLimit, dontLimitStarvable, null);
  }

  /**
   * Constructs a new {@link PrioritySchedulerServiceQueueLimitRejector} with the provided 
   * scheduler, limit, and handler for when task can not be submitted to the pool.  This 
   * constructor additionally allows you to specify if starvable tasks should be included in the 
   * queue limit.  Because starvable tasks have less impact on the pool, the need to limit them 
   * may be reduced.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   * @param dontLimitStarvable Provide {@code true} to don't include starvable tasks against queue limit
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed due to queue size
   */
  public PrioritySchedulerServiceQueueLimitRejector(PrioritySchedulerService parentScheduler, 
                                                    int queuedTaskLimit, boolean dontLimitStarvable, 
                                                    RejectedExecutionHandler rejectedExecutionHandler) {
    super(parentScheduler, queuedTaskLimit, rejectedExecutionHandler);
    
    this.parentScheduler = parentScheduler;
    this.dontLimitStarvable = dontLimitStarvable;
  }
  
  protected void doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    if (dontLimitStarvable && priority == TaskPriority.Starvable) {
      parentScheduler.schedule(task, delayInMillis, priority);
      return;
    }
    
    while (true) {
      int casValue = queuedTaskCount.get();
      if (casValue >= getQueueLimit()) {
        rejectedExecutionHandler.handleRejectedTask(task);
        return; // in case handler did not throw exception
      } else if (queuedTaskCount.compareAndSet(casValue, casValue + 1)) {
        try {
          parentScheduler.schedule(new DecrementingRunnable(task, queuedTaskCount), 
                                   delayInMillis, priority);
        } catch (RejectedExecutionException e) {
          queuedTaskCount.decrementAndGet();
          throw e;
        }
        break;
      } // else loop and retry
    }
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    doSchedule(task, 0, priority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submit(task, null, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return submit(new RunnableCallableAdapter<T>(task, result), priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    doSchedule(lft, 0, priority);
    
    return lft;
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    doSchedule(task, delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return submitScheduled(new RunnableCallableAdapter<T>(task, result), delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    doSchedule(lft, delayInMs, priority);
    
    return lft;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    // we don't track recurring tasks
    parentScheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    // we don't track recurring tasks
    parentScheduler.scheduleAtFixedRate(task, initialDelay, period, priority);
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return parentScheduler.getDefaultPriority();
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return parentScheduler.getMaxWaitForLowPriority();
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    return parentScheduler.getQueuedTaskCount(priority);
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    return parentScheduler.getWaitingForExecutionTaskCount(priority);
  }
}
