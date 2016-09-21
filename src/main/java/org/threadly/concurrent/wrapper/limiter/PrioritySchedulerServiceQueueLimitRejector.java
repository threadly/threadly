package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.CallableContainer;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.limiter.ExecutorQueueLimitRejector.DecrementingRunnable;

/**
 * <p>A simple way to limit any {@link PrioritySchedulerService} so that queues are managed.  In 
 * addition this queue is tracked completely independent of the {@link PrioritySchedulerService}'s 
 * actual queue, so these can be distributed in code to limit queues differently to different parts 
 * of the system, while letting them all back the same {@link PrioritySchedulerService}.</p>
 * 
 * <p>Once the limit has been reached, if additional tasks are supplied a 
 * {@link java.util.concurrent.RejectedExecutionException} will be thrown.  This is the threadly 
 * equivalent of supplying a limited sized blocking queue to a java.util.concurrent thread 
 * pool.</p>
 * 
 * <p>See {@link ExecutorQueueLimitRejector}, {@link SubmitterSchedulerQueueLimitRejector} and 
 * {@link SchedulerServiceQueueLimitRejector} as other possible implementations.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.8.0
 */
public class PrioritySchedulerServiceQueueLimitRejector extends SchedulerServiceQueueLimitRejector 
                                                        implements PrioritySchedulerService {
  protected final PrioritySchedulerService parentScheduler;

  /**
   * Constructs a new {@link PrioritySchedulerServiceQueueLimitRejector} with the provided scheduler and limit.
   * 
   * @param parentScheduler Scheduler to execute and schedule tasks on to
   * @param queuedTaskLimit Maximum number of queued tasks before executions should be rejected
   */
  public PrioritySchedulerServiceQueueLimitRejector(PrioritySchedulerService parentScheduler, int queuedTaskLimit) {
    super(parentScheduler, queuedTaskLimit);
    
    this.parentScheduler = parentScheduler;
  }
  
  protected Runnable wrapTaskIfCanQueue(Runnable task) {
    if (task == null) {
      return null;
    } else if (queuedTaskCount.get() >= getQueueLimit()) {
      throw new RejectedExecutionException();
    } else if (queuedTaskCount.incrementAndGet() > getQueueLimit()) {
      queuedTaskCount.decrementAndGet();
      throw new RejectedExecutionException();
    } else {
      return new DecrementingRunnable(task, queuedTaskCount);
    }
  }
  
  protected <T> Callable<T> wrapTaskIfCanQueue(Callable<T> task) {
    if (task == null) {
      return null;
    } else if (queuedTaskCount.get() >= getQueueLimit()) {
      throw new RejectedExecutionException();
    } else if (queuedTaskCount.incrementAndGet() > getQueueLimit()) {
      queuedTaskCount.decrementAndGet();
      throw new RejectedExecutionException();
    } else {
      return new DecrementingCallable<T>(task, queuedTaskCount);
    }
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    parentScheduler.execute(wrapTaskIfCanQueue(task), priority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return parentScheduler.submit(wrapTaskIfCanQueue(task), priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return parentScheduler.submit(wrapTaskIfCanQueue(task), result, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return parentScheduler.submit(wrapTaskIfCanQueue(task), priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    parentScheduler.schedule(wrapTaskIfCanQueue(task), delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, TaskPriority priority) {
    return parentScheduler.submitScheduled(wrapTaskIfCanQueue(task), delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return parentScheduler.submitScheduled(wrapTaskIfCanQueue(task), result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return parentScheduler.submitScheduled(wrapTaskIfCanQueue(task), delayInMs, priority);
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
  
  /**
   * <p>This callable decrements a provided AtomicInteger at the START of execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.8.0
   */
  protected static class DecrementingCallable<T> implements Callable<T>, CallableContainer<T> {
    private final Callable<T> task;
    private final AtomicInteger queuedTaskCount;
    
    public DecrementingCallable(Callable<T> task, AtomicInteger queuedTaskCount) {
      this.task = task;
      this.queuedTaskCount = queuedTaskCount;
    }

    @Override
    public Callable<T> getContainedCallable() {
      return task;
    }

    @Override
    public T call() throws Exception {
      queuedTaskCount.decrementAndGet();
      return task.call();
    }
  }
}
