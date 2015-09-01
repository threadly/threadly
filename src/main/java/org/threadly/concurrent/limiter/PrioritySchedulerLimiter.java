package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link PrioritySchedulerService}.  This allows the implementor to have one thread pool for 
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
@SuppressWarnings("deprecation")
public class PrioritySchedulerLimiter extends SchedulerServiceLimiter 
                                      implements PrioritySchedulerInterface {
  protected final PrioritySchedulerService scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerService}.
   * 
   * @param scheduler {@link PrioritySchedulerService} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public PrioritySchedulerLimiter(PrioritySchedulerService scheduler, 
                                  int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerService}.
   * 
   * @param scheduler {@link PrioritySchedulerService} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  public PrioritySchedulerLimiter(PrioritySchedulerService scheduler, 
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
      scheduler.schedule(new DelayedExecutionRunnable(new PriorityWrapper(task, priority)), 
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
    
    RecurringDelayWrapper rdw = new RecurringDelayWrapper(task, recurringDelay, priority);
    
    if (initialDelay == 0) {
      executeWrapper(rdw);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable(rdw), 
                         initialDelay, TaskPriority.High);
    }
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    RecurringRateWrapper rrw = new RecurringRateWrapper(task, initialDelay, period, priority);
    
    if (initialDelay == 0) {
      executeWrapper(rrw);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable(rrw), 
                         initialDelay, TaskPriority.High);
    }
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }

  @Override
  public long getMaxWaitForLowPriority() {
    return scheduler.getMaxWaitForLowPriority();
  }

  /**
   * <p>Wrapper for recurring tasks that reschedule with a given delay after completing 
   * execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringDelayWrapper extends PriorityWrapper {
    protected final long recurringDelay;
    protected final DelayedExecutionRunnable delayRunnable;
    
    protected RecurringDelayWrapper(Runnable runnable, 
                                    long recurringDelay, 
                                    TaskPriority priority) {
      super(runnable, priority);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable(this);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay, TaskPriority.High);
    }
  }

  /**
   * <p>Wrapper for recurring tasks that reschedule at a fixed rate after completing execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringRateWrapper extends PriorityWrapper {
    protected final long period;
    protected final DelayedExecutionRunnable delayRunnable;
    private long nextRunTime;
    
    protected RecurringRateWrapper(Runnable runnable, 
                                   long initialDelay, long period, 
                                   TaskPriority priority) {
      super(runnable, priority);
      
      this.period = period;
      delayRunnable = new DelayedExecutionRunnable(this);
      nextRunTime = Clock.accurateForwardProgressingMillis() + initialDelay + period;
    }
    
    @Override
    protected void doAfterRunTasks() {
      nextRunTime += period;
      long nextDelay = nextRunTime - Clock.accurateForwardProgressingMillis();
      if (nextDelay < 1) {
        executeWrapper(this);
      } else {
        scheduler.schedule(delayRunnable, nextDelay, TaskPriority.High);
      }
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
    protected final TaskPriority priority;
    
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
