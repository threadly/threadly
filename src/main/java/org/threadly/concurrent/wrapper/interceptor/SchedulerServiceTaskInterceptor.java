package org.threadly.concurrent.wrapper.interceptor;

import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import org.threadly.concurrent.SchedulerService;

/**
 * <p>Class to wrap {@link SchedulerService} pool so that tasks can be intercepted and either 
 * wrapped, or modified, before being submitted to the pool.  This class can be passed a lambda to
 * {@link #SchedulerServiceTaskInterceptor(SchedulerService, BiFunction)}}, or 
 * {@link #wrapTask(Runnable, boolean)} can be overridden to provide the task which should be 
 * submitted to the {@link SchedulerService}.  Please see the javadocs of 
 * {@link #wrapTask(Runnable, boolean)} for more details about ways a task can be modified or 
 * wrapped.</p>
 * 
 * <p>Other variants of task wrappers: {@link ExecutorTaskInterceptor}, 
 * {@link SubmitterSchedulerTaskInterceptor}, {@link PrioritySchedulerTaskInterceptor}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0
 */
public class SchedulerServiceTaskInterceptor extends SubmitterSchedulerTaskInterceptor 
                                             implements SchedulerService {
  protected final SchedulerService parentScheduler;

  /**
   * When using this constructor, {@link #wrapTask(Runnable, boolean)} must be overridden to 
   * handle the task manipulation before the task is submitted to the provided 
   * {@link SchedulerService}.  Please see the javadocs of {@link #wrapTask(Runnable, boolean)} 
   * for more details about ways a task can be modified or wrapped.
   * 
   * @param parentExecutor An instance of {@link Executor} to wrap
   */
  protected SchedulerServiceTaskInterceptor(SchedulerService parentScheduler) {
    this(parentScheduler, null);
  }
  
  /**
   * Constructs a wrapper for {@link SchedulerService} pool so that tasks can be intercepted and modified,
   * before being submitted to the pool.
   * 
   * @param parentScheduler An instance of {@link SchedulerService} to wrap
   * @param taskManipulator A lambda to manipulate a {@link Runnable} that was submitted for execution
   */
  public SchedulerServiceTaskInterceptor(SchedulerService parentScheduler, 
                                         BiFunction<Runnable, Boolean, Runnable> taskManipulator) {
    super(parentScheduler, taskManipulator);
    
    this.parentScheduler = parentScheduler;
  }

  @Override
  public boolean remove(Runnable task) {
    return parentScheduler.remove(task);
  }

  @Override
  public boolean remove(Callable<?> task) {
    return parentScheduler.remove(task);
  }

  @Override
  public int getActiveTaskCount() {
    return parentScheduler.getActiveTaskCount();
  }

  @Override
  public int getQueuedTaskCount() {
    return parentScheduler.getQueuedTaskCount();
  }

  @Override
  public boolean isShutdown() {
    return parentScheduler.isShutdown();
  }
}
