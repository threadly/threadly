package org.threadly.concurrent.wrapper.priority;

import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * Wrapper which will delegate the tasks provided to a specific scheduler designated for their 
 * priority.  This implements the {@link PrioritySchedulerService} with a collection of multiple 
 * executors (and queues) for each priority rather than a single queue.
 * <p>
 * One useful example of this could be in producing a priority aware implementation of 
 * {@link org.threadly.concurrent.wrapper.limiter.SchedulerServiceLimiter} where different 
 * priorities are allowed to use a given portion of the pool.
 * <p>
 * Because of the use of multiple pools (often times backed by a single pool), this class does have 
 * some unique considerations.  Most of the non-task execution specific functions of 
 * {@link PrioritySchedulerService} will have some unique behaviors.  Specifically 
 * {@link #getMaxWaitForLowPriority()} will always return zero because there is no ability to 
 * balance the task queues between the different schedulers.  In addition functions like 
 * {@link #getActiveTaskCount()}, {@link #getQueuedTaskCount()}, 
 * {@link #getWaitingForExecutionTaskCount()} will return the values from the default priority's 
 * scheduler (since it is assumed that the provided schedulers may be multiple of the same reference, 
 * or may delegate to the same parent pool).
 * 
 * @since 5.8
 */
public class PriorityDelegatingScheduler extends AbstractSubmitterScheduler
                                         implements PrioritySchedulerService {
  private final SchedulerService highPriorityScheduler;
  private final SchedulerService lowPriorityScheduler;
  private final SchedulerService starvablePriorityScheduler;
  private final TaskPriority defaultPriority;

  /**
   * Construct a new {@link PrioritySchedulerService} that delegates to the given schedulers.
   * <p>
   * Please see class documentation to understand how {@code defaultPriority} will also provide the 
   * priority for the non-task execution portions of the {@link SchedulerService} interface.
   * 
   * @param highPriorityScheduler Scheduler to be used for high priority tasks
   * @param lowPriorityScheduler Scheduler to be used for low priority tasks
   * @param starvablePriorityScheduler Scheduler to be used for starvable priority tasks, 
   *                                     or {@code null} to delegate to {@code lowPriorityScheduler}
   * @param defaultPriority The default priority to be used when none is provided
   */
  public PriorityDelegatingScheduler(SchedulerService highPriorityScheduler, 
                                     SchedulerService lowPriorityScheduler, 
                                     SchedulerService starvablePriorityScheduler, 
                                     TaskPriority defaultPriority) {
    ArgumentVerifier.assertNotNull(highPriorityScheduler, "highPriorityScheduler");
    ArgumentVerifier.assertNotNull(lowPriorityScheduler, "lowPriorityScheduler");
    ArgumentVerifier.assertNotNull(defaultPriority, "defaultPriority");
    
    this.highPriorityScheduler = highPriorityScheduler;
    this.lowPriorityScheduler = lowPriorityScheduler;
    this.starvablePriorityScheduler = 
        starvablePriorityScheduler == null ? lowPriorityScheduler : starvablePriorityScheduler;
    this.defaultPriority = defaultPriority;
  }
  
  /**
   * Get a scheduler of the specified priority.
   * 
   * @param priority Desired priority or {@code null} for default priority
   * @return Scheduler which corresponds to the priority requested
   */
  protected SchedulerService scheduler(TaskPriority priority) {
    if (priority == TaskPriority.High) {
      return highPriorityScheduler;
    } else if (priority == TaskPriority.Low) {
      return lowPriorityScheduler;
    } else if (priority == TaskPriority.Starvable) {
      return starvablePriorityScheduler;
    } else {
      // priority must be null
      return scheduler(defaultPriority);
    }
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return 0;
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    return scheduler(priority).getQueuedTaskCount();
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    return scheduler(priority).getWaitingForExecutionTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return highPriorityScheduler.isShutdown() | 
             lowPriorityScheduler.isShutdown() | starvablePriorityScheduler.isShutdown();
  }

  @Override
  public int getQueuedTaskCount() {
    return scheduler(defaultPriority).getQueuedTaskCount();
  }

  @Override
  public int getWaitingForExecutionTaskCount() {
    return scheduler(defaultPriority).getWaitingForExecutionTaskCount();
  }

  @Override
  public int getActiveTaskCount() {
    return scheduler(defaultPriority).getActiveTaskCount();
  }

  @Override
  public boolean remove(Runnable task) {
    if (highPriorityScheduler.remove(task)) {
      return true;
    } else if (lowPriorityScheduler != highPriorityScheduler && lowPriorityScheduler.remove(task)) {
      return true;
    } else if (starvablePriorityScheduler != highPriorityScheduler && 
                 starvablePriorityScheduler != lowPriorityScheduler && 
                 starvablePriorityScheduler.remove(task)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean remove(Callable<?> task) {
    if (highPriorityScheduler.remove(task)) {
      return true;
    } else if (lowPriorityScheduler != highPriorityScheduler && lowPriorityScheduler.remove(task)) {
      return true;
    } else if (starvablePriorityScheduler != highPriorityScheduler && 
                 starvablePriorityScheduler != lowPriorityScheduler && 
                 starvablePriorityScheduler.remove(task)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMs) {
    scheduler(defaultPriority).schedule(task, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduler(defaultPriority).scheduleWithFixedDelay(task, initialDelay, recurringDelay);
    
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduler(defaultPriority).scheduleAtFixedRate(task, initialDelay, period);
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    scheduler(priority).execute(task);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return scheduler(priority).submit(task, result);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return scheduler(priority).submit(task);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    scheduler(priority).schedule(task, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return scheduler(priority).submitScheduled(task, result, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return scheduler(priority).submitScheduled(task, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    scheduler(priority).scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    scheduler(priority).scheduleAtFixedRate(task, initialDelay, period);
  }
}
