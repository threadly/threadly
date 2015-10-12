package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.Promise;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link SubmitterScheduler}.  This allows the implementor to have one thread pool for all 
 * their code, and if they want certain sections to have less levels of parallelism (possibly 
 * because those those sections would completely consume the global pool), they can wrap the 
 * executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
public class SubmitterSchedulerLimiter extends ExecutorLimiter 
                                       implements SubmitterScheduler {
  protected final SubmitterScheduler scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterScheduler}.
   * 
   * @param scheduler {@link SubmitterScheduler} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterScheduler}.
   * 
   * @deprecated Rename threads using {@link org.threadly.concurrent.ThreadRenamingSubmitterSchedulerWrapper} 
   *               to rename executions from this limiter
   * 
   * @param scheduler {@link SubmitterScheduler} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  @Deprecated
  public SubmitterSchedulerLimiter(SubmitterScheduler scheduler, 
                                   int maxConcurrency, String subPoolName) {
    super(scheduler, maxConcurrency, subPoolName);
    
    this.scheduler = scheduler;
  }

  @Override
  public Promise<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> Promise<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return submitScheduled(new RunnableCallableAdapter<T>(task, result), delayInMs);
  }

  @Override
  public <T> Promise<T> submitScheduled(Callable<T> task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<T>(false, task);
    
    doSchedule(ft, delayInMs);
    
    return ft;
  }
  
  /**
   * Adds a task to either execute (delay zero), or schedule with the provided delay.  No safety 
   * checks are done at this point, so only provide non-null inputs.
   * 
   * @param task Task for execution
   * @param delayInMs delay in milliseconds, greater than or equal to zero
   */
  protected void doSchedule(Runnable task, long delayInMs) {
    if (delayInMs == 0) {
      doExecute(task);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable(task), delayInMs);
    }
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    doSchedule(task, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    doSchedule(new RecurringDelayWrapper(task, recurringDelay), initialDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    
    doSchedule(new RecurringRateWrapper(task, initialDelay, period), initialDelay);
  }
  
  /**
   * <p>Small runnable that allows scheduled tasks to pass through the same execution queue that 
   * immediate execution has to.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.1.0
   */
  protected class DelayedExecutionRunnable implements Runnable, RunnableContainer {
    private final LimiterRunnableWrapper lrw;

    protected DelayedExecutionRunnable(Runnable runnable) {
      this(new LimiterRunnableWrapper(executor, runnable));
    }

    protected DelayedExecutionRunnable(LimiterRunnableWrapper lrw) {
      this.lrw = lrw;
    }
    
    @Override
    public void run() {
      if (canRunTask()) {  // we can run in the thread we already have
        lrw.run();
      } else {
        addToQueue(lrw);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return lrw.getContainedRunnable();
    }
  }

  /**
   * <p>Wrapper for recurring tasks that reschedule with a given delay after completing 
   * execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringDelayWrapper extends LimiterRunnableWrapper {
    protected final long recurringDelay;
    protected final DelayedExecutionRunnable delayRunnable;
    
    public RecurringDelayWrapper(Runnable runnable, long recurringDelay) {
      super(scheduler, runnable);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable(this);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay);
    }
  }

  /**
   * <p>Wrapper for recurring tasks that reschedule at a fixed rate after completing execution.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringRateWrapper extends LimiterRunnableWrapper {
    protected final long period;
    protected final DelayedExecutionRunnable delayRunnable;
    private long nextRunTime;
    
    public RecurringRateWrapper(Runnable runnable, long initialDelay, long period) {
      super(scheduler, runnable);
      
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
        scheduler.schedule(delayRunnable, nextDelay);
      }
    }
  }
}
