package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.VirtualRunnable;

/**
 * This class is designed to limit how much parallel execution happens 
 * on a provided {@link SimpleSchedulerInterface}.  This allows the 
 * implementor to have one thread pool for all their code, and if 
 * they want certain sections to have less levels of parallelism 
 * (possibly because those those sections would completely consume the 
 * global pool), they can wrap the executor in this class.
 * 
 * Thus providing you better control on the absolute thread count and 
 * how much parallelism can occur in different sections of the program.  
 * 
 * Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.
 * 
 * @author jent - Mike Jensen
 */
public class SimpleSchedulerLimiter extends ExecutorLimiter 
                                    implements SimpleSchedulerInterface {
  protected final SimpleSchedulerInterface scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SimpleSchedulerInterface}.
   * 
   * @param scheduler {@link SimpleSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public SimpleSchedulerLimiter(SimpleSchedulerInterface scheduler, 
                                int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SimpleSchedulerInterface}.
   * 
   * @param scheduler {@link SimpleSchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool (null to not change thread names)
   */
  public SimpleSchedulerLimiter(SimpleSchedulerInterface scheduler, 
                                int maxConcurrency, String subPoolName) {
    super(scheduler, maxConcurrency, subPoolName);
    
    this.scheduler = scheduler;
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    if (delayInMs == 0) {
      execute(task);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(task), 
                         delayInMs);
    }
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    
    RecurringRunnableWrapper rrw = new RecurringRunnableWrapper(task, recurringDelay);
    
    if (initialDelay == 0) {
      execute(rrw);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(rrw), 
                         initialDelay);
    }
  }
  
  /**
   * Small runnable that allows scheduled tasks to pass through 
   * the same execution queue that immediate execution has to.
   * 
   * @author jent - Mike Jensen
   */
  protected class DelayedExecutionRunnable<T> extends VirtualRunnable {
    private final Runnable runnable;

    public DelayedExecutionRunnable(Runnable runnable) {
      this.runnable = runnable;
    }
    
    @Override
    public void run() {
      execute(runnable);
    }
  }

  /**
   * Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class RecurringRunnableWrapper extends LimiterRunnableWrapper {
    private final long recurringDelay;
    private final DelayedExecutionRunnable<?> delayRunnable;
    
    public RecurringRunnableWrapper(Runnable runnable, 
                                    long recurringDelay) {
      super(runnable);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable<Object>(this);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay);
    }
  }
}
