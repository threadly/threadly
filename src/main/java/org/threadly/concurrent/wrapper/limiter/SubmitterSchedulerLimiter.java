package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * This class is designed to limit how much parallel execution happens on a provided 
 * {@link SubmitterScheduler}.  This allows the implementor to have one thread pool for all 
 * their code, and if they want certain sections to have less levels of parallelism (possibly 
 * because those those sections would completely consume the global pool), they can wrap the 
 * executor in this class.
 * <p>
 * Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.
 * <p>
 * This is an alternative from having to create multiple thread pools.  By using this you also are 
 * able to accomplish more efficiently thread use than multiple thread pools would.
 * 
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class SubmitterSchedulerLimiter extends ExecutorLimiter implements SubmitterScheduler {
  protected final SubmitterScheduler scheduler;
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterScheduler}.
   * 
   * @param scheduler {@link SubmitterScheduler} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public SubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Constructs a new limiter that implements the {@link SubmitterScheduler}.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param scheduler {@link SubmitterScheduler} implementation to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public SubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency, 
                                   boolean limitFutureListenersExecution) {
    super(scheduler, maxConcurrency, limitFutureListenersExecution);
    
    this.scheduler = scheduler;
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return submitScheduled(new RunnableCallableAdapter<>(task, result), delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<>(false, task);
    
    doSchedule(ft, ft, delayInMs);
    
    return ft;
  }
  
  /**
   * Adds a task to either execute (delay zero), or schedule with the provided delay.  No safety 
   * checks are done at this point, so only provide non-null inputs.
   * 
   * @param task Task for execution
   * @param delayInMs delay in milliseconds, greater than or equal to zero
   */
  protected void doSchedule(Runnable task, ListenableFuture<?> future, long delayInMs) {
    if (delayInMs == 0) {
      executeOrQueue(task, future);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable(task), delayInMs);
    }
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    doSchedule(task, null, delayInMs);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    doSchedule(new RecurringDelayWrapper(task, recurringDelay), null, initialDelay);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    
    doSchedule(new RecurringRateWrapper(task, initialDelay, period), null, initialDelay);
  }
  
  /**
   * Small runnable that allows scheduled tasks to pass through the same execution queue that 
   * immediate execution has to.
   * 
   * @since 1.1.0
   */
  protected class DelayedExecutionRunnable implements Runnable, RunnableContainer {
    private final LimiterRunnableWrapper lrw;

    protected DelayedExecutionRunnable(Runnable runnable) {
      this(new LimiterRunnableWrapper(runnable));
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
   * Wrapper for recurring tasks that reschedule with a given delay after completing execution.
   * 
   * @since 3.1.0
   */
  protected class RecurringDelayWrapper extends LimiterRunnableWrapper {
    protected final long recurringDelay;
    protected final DelayedExecutionRunnable delayRunnable;
    
    public RecurringDelayWrapper(Runnable runnable, long recurringDelay) {
      super(runnable);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable(this);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay);
    }
  }

  /**
   * Wrapper for recurring tasks that reschedule at a fixed rate after completing execution.
   * 
   * @since 3.1.0
   */
  protected class RecurringRateWrapper extends LimiterRunnableWrapper {
    protected final long period;
    protected final DelayedExecutionRunnable delayRunnable;
    private long nextRunTime;
    
    public RecurringRateWrapper(Runnable runnable, long initialDelay, long period) {
      super(runnable);
      
      this.period = period;
      delayRunnable = new DelayedExecutionRunnable(this);
      nextRunTime = Clock.accurateForwardProgressingMillis() + initialDelay + period;
    }
    
    @Override
    protected void doAfterRunTasks() {
      nextRunTime += period;
      long nextDelay = nextRunTime - Clock.accurateForwardProgressingMillis();
      if (nextDelay < 1) {
        executeOrQueueWrapper(this);
      } else {
        scheduler.schedule(delayRunnable, nextDelay);
      }
    }
  }
}
