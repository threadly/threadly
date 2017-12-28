package org.threadly.concurrent.wrapper.traceability;

import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * Class which wraps a {@link PrioritySchedulerService} and wraps all supplied tasks in a 
 * {@link ThreadRenamingRunnable}.  This allows you to make a pool where all tasks submitted 
 * inside it have the threads named in an identifiable way.
 * 
 * @since 5.7
 */
public class ThreadRenamingPriorityScheduler extends AbstractSubmitterScheduler 
                                             implements PrioritySchedulerService {
  protected final PrioritySchedulerService scheduler;
  protected final String threadName;
  protected final boolean replace;

  /**
   * Constructs a new {@link ThreadRenamingPriorityScheduler}, wrapping a supplied 
   * {@link PrioritySchedulerService}.  If {@code replace} is {@code false} the thread will be 
   * named such that {@code threadName[originalThreadName]}.
   * 
   * @param scheduler SubmitterScheduler to wrap and send executions to
   * @param threadName Thread name prefix, or replaced name
   * @param replace If {@code true} the original name wont be included in the thread name
   */
  public ThreadRenamingPriorityScheduler(PrioritySchedulerService scheduler, String threadName,
                                         boolean replace) {
    this.scheduler = scheduler;
    this.threadName = threadName;
    this.replace = replace;
  }

  @Override
  public boolean remove(Runnable task) {
    return scheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return scheduler.remove(task);
  }

  @Override
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  @Override
  public int getQueuedTaskCount() {
    return scheduler.getQueuedTaskCount();
  }

  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    return scheduler.getQueuedTaskCount(priority);
  }

  @Override
  public int getWaitingForExecutionTaskCount() {
    return scheduler.getWaitingForExecutionTaskCount();
  }

  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    return scheduler.getWaitingForExecutionTaskCount(priority);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return scheduler.getMaxWaitForLowPriority();
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return submitScheduled(task, result, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");

    doSchedule(task, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return submitScheduled(new RunnableCallableAdapter<>(task, result), delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<>(false, task, this);
    doSchedule(rf, delayInMs, priority);
    
    return rf;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, null);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    scheduler.scheduleWithFixedDelay(new ThreadRenamingRunnable(task, threadName, replace), 
                                     initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduleAtFixedRate(task, initialDelay, period, null);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    scheduler.scheduleAtFixedRate(new ThreadRenamingRunnable(task, threadName, replace), 
                                  initialDelay, period, priority);
    
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    doSchedule(task, delayInMillis, null);
  }
  
  protected void doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    scheduler.schedule(new ThreadRenamingRunnable(task, threadName, replace), 
                       delayInMillis, priority);
  }
}
