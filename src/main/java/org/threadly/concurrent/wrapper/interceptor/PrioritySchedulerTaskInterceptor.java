package org.threadly.concurrent.wrapper.interceptor;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Class to wrap {@link PrioritySchedulerService} pool so that tasks can be intercepted and either 
 * wrapped, or modified, before being submitted to the pool.  This class can be passed a lambda to
 * {@link #PrioritySchedulerTaskInterceptor(PrioritySchedulerService, BiFunction)}}, or 
 * {@link #wrapTask(Runnable, boolean)} can be overridden to provide the task which should be submitted 
 * to the {@link PrioritySchedulerService}.  Please see the javadocs of 
 * {@link #wrapTask(Runnable, boolean)} for more details about ways a task can be modified or 
 * wrapped.</p>
 * 
 * <p>Other variants of task wrappers: {@link ExecutorTaskInterceptor}, 
 * {@link SubmitterSchedulerTaskInterceptor}, {@link PrioritySchedulerTaskInterceptor}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0
 */
public class PrioritySchedulerTaskInterceptor extends SchedulerServiceTaskInterceptor 
                                              implements PrioritySchedulerService {
  protected final PrioritySchedulerService parentScheduler;

  /**
   * When using this constructor, {@link #wrapTask(Runnable, boolean)} must be overridden to 
   * handle the task manipulation before the task is submitted to the provided 
   * {@link PrioritySchedulerService}.  Please see the javadocs of 
   * {@link #wrapTask(Runnable, boolean)} for more details about ways a task can be modified or 
   * wrapped.
   * 
   * @param parentExecutor An instance of {@link Executor} to wrap
   */
  protected PrioritySchedulerTaskInterceptor(PrioritySchedulerService parentScheduler) {
    this(parentScheduler, null);
  }
  
  /**
   * Constructs a wrapper for {@link PrioritySchedulerService} pool so that tasks can be intercepted 
   * and modified, before being submitted to the pool.
   * 
   * @param parentScheduler An instance of {@link PrioritySchedulerService} to wrap
   * @param taskManipulator A lambda to manipulate a {@link Runnable} that was submitted for execution
   */
  public PrioritySchedulerTaskInterceptor(PrioritySchedulerService parentScheduler,
                                          BiFunction<Runnable, Boolean, Runnable> taskManipulator) {
    super(parentScheduler, taskManipulator);
    
    this.parentScheduler = parentScheduler;
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    parentScheduler.execute(task == null ? null : wrapTask(task, false), priority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submit(task, null, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return parentScheduler.submit(task == null ? null : wrapTask(task, false), result, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    parentScheduler.execute(wrapTask(lft, false), priority);
    
    return lft;
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    parentScheduler.schedule(task == null ? null : wrapTask(task, false), delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return parentScheduler.submitScheduled(task == null ? null : wrapTask(task, false), 
                                           result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);

    parentScheduler.schedule(wrapTask(lft, false), delayInMs, priority);
    
    return lft;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    parentScheduler.scheduleWithFixedDelay(task == null ? null : wrapTask(task, true), 
                                           initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    parentScheduler.scheduleAtFixedRate(task == null ? null : wrapTask(task, true), 
                                        initialDelay, period, priority);
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return parentScheduler.getDefaultPriority();
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return parentScheduler.getMaxWaitForLowPriority();
  }
}
