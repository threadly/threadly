package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link PrioritySchedulerInterface}.  This allows the implementor to have one thread pool for 
 * all their code, and if they want certain sections to have less levels of parallelism 
 * (possibly because those those sections would completely consume the global pool), they can wrap 
 * the executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class PrioritySchedulerLimiter extends SchedulerServiceLimiter 
                                      implements PrioritySchedulerInterface {
  protected final PrioritySchedulerInterface scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerInterface}.
   * 
   * @param scheduler {@link PrioritySchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public PrioritySchedulerLimiter(PrioritySchedulerInterface scheduler, 
                                  int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerInterface}.
   * 
   * @param scheduler {@link PrioritySchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  public PrioritySchedulerLimiter(PrioritySchedulerInterface scheduler, 
                                  int maxConcurrency, String subPoolName) {
    super(scheduler, maxConcurrency, subPoolName);
    
    this.scheduler = scheduler;
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    PriorityWrapper pw = new PriorityWrapper(task, priority);
    executeWrapper(pw);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submitScheduled(task, null, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, 
                                        TaskPriority priority) {
    return submitScheduled(task, result, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, 
                                        TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs, 
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<T>(false, task, result);
    
    schedule(ft, delayInMs, priority);
    
    return ft;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<T>(false, task);
    
    schedule(ft, delayInMs, priority);
    
    return ft;
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    if (delayInMs == 0) {
      execute(task, priority);
    } else {
      scheduler.schedule(new PriorityDelayedRunnable(task, priority), 
                         delayInMs, TaskPriority.High);
    }
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    RecurringRunnableWrapper rrw = new RecurringRunnableWrapper(task, recurringDelay, priority);
    
    if (initialDelay == 0) {
      executeWrapper(rrw);
    } else {
      scheduler.schedule(new PriorityDelayedRunnable(rrw, priority), 
                         initialDelay, TaskPriority.High);
    }
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }
  
  /**
   * <p>Small runnable that allows scheduled tasks to pass through the same execution queue that 
   * immediate execution has to.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.1.0
   */
  protected class PriorityDelayedRunnable extends DelayedExecutionRunnable {
    protected PriorityDelayedRunnable(Runnable runnable, TaskPriority priority) {
      super(new PriorityWrapper(runnable, priority));
    }
  }

  /**
   * <p>Wrapper for tasks which are executed in this sub pool, this ensures that 
   * {@link #handleTaskFinished()} will be called after the task completes.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.1.0
   */
  protected class RecurringRunnableWrapper extends PriorityWrapper {
    private final long recurringDelay;
    private final PriorityDelayedRunnable delayRunnable;
    
    protected RecurringRunnableWrapper(Runnable runnable, 
                                       long recurringDelay, 
                                       TaskPriority priority) {
      super(runnable, priority);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new PriorityDelayedRunnable(this, priority);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay, TaskPriority.High);
    }
  }

  /**
   * <p>Class that adds {@link TaskPriority} functionality to the 
   * {@link LimiterRunnableWrapper}.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.1.0
   */
  protected class PriorityWrapper extends LimiterRunnableWrapper {
    private final TaskPriority priority;
    
    protected PriorityWrapper(Runnable runnable, TaskPriority priority) {
      super(scheduler, runnable);
      
      this.priority = priority;
    }
    
    @Override
    protected void submitToExecutor() {
      scheduler.execute(this, priority);
    }
  }
}
