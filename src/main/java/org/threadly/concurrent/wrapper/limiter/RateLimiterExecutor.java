package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ImmediateResultListenableFuture;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * <p>Another way to limit executions on a scheduler.  Unlike the {@link ExecutorLimiter} this 
 * does not attempt to limit concurrency.  Instead it schedules tasks on a scheduler so that given 
 * permits are only used at a rate per second.  This can be used for limiting the rate of data 
 * that you want to put on hardware resource (in a non-blocking way).</p>
 * 
 * <p>It is important to note that if something is executed and it exceeds the rate, it will be 
 * future tasks which are delayed longer.</p>
 * 
 * <p>It is also important to note that it is the responsibility of the application to not be 
 * providing more tasks into this limiter than can be consumed at the rate.  Since this limiter 
 * will not block, if provided tasks too fast they could continue to be scheduled out further and 
 * further.  This should be used to flatten out possible bursts that could be used in the 
 * application, it is not designed to be a push back mechanism for the application.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 2.0.0 at org.threadly.concurrent.limiter)
 */
public class RateLimiterExecutor extends AbstractSubmitterExecutor {
  protected final SubmitterScheduler scheduler;
  protected final RejectedExecutionHandler rejectedExecutionHandler;
  protected final Object permitLock;
  protected volatile double permitsPerSecond;
  protected volatile long maxScheduleDelayMillis;
  private double lastScheduleTime;
  
  /**
   * Constructs a new {@link RateLimiterExecutor}.  Tasks will be scheduled on the provided 
   * scheduler, so it is assumed that the scheduler will have enough threads to handle the 
   * average permit amount per task, per second.  
   * 
   * This will schedule tasks out infinitely far in order to maintain rate.  If you want tasks to 
   * be rejected at a certain point consider using 
   * {@link #RateLimiterExecutor(SubmitterScheduler, double, long)}.
   * 
   * @param scheduler scheduler to schedule/execute tasks on
   * @param permitsPerSecond how many permits should be allowed per second
   */
  public RateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond) {
    this(scheduler, permitsPerSecond, Long.MAX_VALUE);
  }
  
  /**
   * Constructs a new {@link RateLimiterExecutor}.  Tasks will be scheduled on the provided 
   * scheduler, so it is assumed that the scheduler will have enough threads to handle the 
   * average permit amount per task, per second.  
   * 
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * 
   * @since 4.8.0
   * @param scheduler scheduler to schedule/execute tasks on
   * @param permitsPerSecond how many permits should be allowed per second
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   */
  public RateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                             long maxScheduleDelayMillis) {
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, null);
  }
  
  /**
   * Constructs a new {@link RateLimiterExecutor}.  Tasks will be scheduled on the provided 
   * scheduler, so it is assumed that the scheduler will have enough threads to handle the 
   * average permit amount per task, per second.  
   * 
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then the provided {@link RejectedExecutionHandler} will be invoked.
   * 
   * @since 4.8.0
   * @param scheduler scheduler to schedule/execute tasks on
   * @param permitsPerSecond how many permits should be allowed per second
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed
   */
  public RateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                             long maxScheduleDelayMillis, 
                             RejectedExecutionHandler rejectedExecutionHandler) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    
    this.scheduler = scheduler;
    if (rejectedExecutionHandler == null) {
      rejectedExecutionHandler = RejectedExecutionHandler.THROW_REJECTED_EXECUTION_EXCEPTION;
    }
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    this.permitLock = new Object();
    this.lastScheduleTime = Clock.lastKnownForwardProgressingMillis();
    setPermitsPerSecond(permitsPerSecond);
    setMaxScheduleDelayMillis(maxScheduleDelayMillis);
  }
  
  /**
   * Sets the allowed permits per second.  When this rate is updated, it only applies to future 
   * submitted task.  In addition if the rate has already been exceeded (and thus there is a delay 
   * in scheduling future items), that delay based off the previous permit rate will not be 
   * adjusted.  For example if the rate was {@code 1/sec} and a 10 permit task was just submitted, 
   * and thus we have a delay of 10 seconds for a future task.  Adjusting this higher will NOT 
   * reduce the delay time for the next task, it will only effect schedule rates after currently 
   * scheduled tasks have been satisfied.
   *  
   * @since 4.6.3
   * @param permitsPerSecond how many permits should be allowed per second
   */
  public void setPermitsPerSecond(double permitsPerSecond) {
    ArgumentVerifier.assertGreaterThanZero(permitsPerSecond, "permitsPerSecond");
    
    this.permitsPerSecond = permitsPerSecond;
  }
  
  /**
   * At runtime adjust the maximum amount that this rate limiter will be willing to schedule out 
   * tasks in order to maintain the rate.  This value must be greater than zero.
   * 
   * @since 4.8.0
   * @param maxScheduleDelayMillis Maximum task delay in milliseconds
   */
  public void setMaxScheduleDelayMillis(long maxScheduleDelayMillis) {
    ArgumentVerifier.assertGreaterThanZero(maxScheduleDelayMillis, "maxScheduleDelayMillis");
    
    this.maxScheduleDelayMillis = maxScheduleDelayMillis;
  }
  
  /**
   * This call will check how far out we have already scheduled tasks to be run.  Because it is 
   * the applications responsibility to not provide tasks too fast for the limiter to run them, 
   * this can give an idea of how backed up tasks provided through this limiter actually are.
   * 
   * @return minimum delay in milliseconds for the next task to be provided
   */
  public int getMinimumDelay() {
    double accurateDelayMillis;
    synchronized (permitLock) {
      accurateDelayMillis = lastScheduleTime - Clock.lastKnownForwardProgressingMillis();
    }
    return (int)Math.max(0, Math.ceil(accurateDelayMillis));
  }
  
  /**
   * In order to help assist with avoiding to schedule too much on the scheduler at any given 
   * time, this call returns a future that will block until the delay for the next task falls 
   * below the maximum delay provided into this call.  If you want to ensure that the next task 
   * will execute immediately, you should provide a zero to this function.  If more tasks are 
   * added to the limiter after this call, it will NOT impact when this future will unblock.  So 
   * this future is assuming that nothing else is added to the limiter after requested.
   * 
   * @param maximumDelay maximum delay in milliseconds until returned Future should unblock
   * @return Future that will unblock {@code get()} calls once delay has been reduced below the provided maximum
   */
  public ListenableFuture<?> getFutureTillDelay(long maximumDelay) {
    int currentMinimumDelay = getMinimumDelay();
    if (currentMinimumDelay == 0) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    } else {
      ListenableFutureTask<?> lft = new ListenableFutureTask<Void>(false, DoNothingRunnable.instance());
      
      long futureDelay;
      if (maximumDelay > 0 && currentMinimumDelay > maximumDelay) {
        futureDelay = maximumDelay;
      } else {
        futureDelay = currentMinimumDelay;
      }
      
      scheduler.schedule(lft, futureDelay);
      
      return lft;
    }
  }
  
  /**
   * Exact same as execute counter part, except you can specify how many permits this task will 
   * require/use (instead of defaulting to 1).  The task will be scheduled out as far as necessary 
   * to ensure it conforms to the set rate.
   * 
   * @param permits resource permits for this task
   * @param task Runnable to execute when ready
   * @return Time in milliseconds task was delayed to maintain rate, or {@code -1} if rejected but handler did not throw
   */
  public long execute(double permits, Runnable task) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(permits, "permits");
    
    return doExecute(permits, task);
  }

  /**
   * Exact same as the submit counter part, except you can specify how many permits this task will 
   * require/use (instead of defaulting to 1).  The task will be scheduled out as far as necessary 
   * to ensure it conforms to the set rate.
   * 
   * @param permits resource permits for this task
   * @param task Runnable to execute when ready
   * @return Future that will indicate when the execution of this task has completed
   */
  public ListenableFuture<?> submit(double permits, Runnable task) {
    return submit(permits, task, null);
  }

  /**
   * Exact same as the submit counter part, except you can specify how many permits this task will 
   * require/use (instead of defaulting to 1).  The task will be scheduled out as far as necessary 
   * to ensure it conforms to the set rate.
   * 
   * @param <T> type of result returned from the future
   * @param permits resource permits for this task
   * @param task Runnable to execute when ready
   * @param result result to return from future when task completes
   * @return Future that will return provided result when the execution has completed
   */
  public <T> ListenableFuture<T> submit(double permits, Runnable task, T result) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(permits, "permits");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    doExecute(permits, lft);
    
    return lft;
  }

  /**
   * Exact same as the submit counter part, except you can specify how many permits this task will 
   * require/use (instead of defaulting to 1).  The task will be scheduled out as far as necessary 
   * to ensure it conforms to the set rate.
   * 
   * @param <T> type of result returned from the future
   * @param permits resource permits for this task
   * @param task Callable to execute when ready
   * @return Future that will return the callables provided result when the execution has completed
   */
  public <T> ListenableFuture<T> submit(double permits, Callable<T> task) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(permits, "permits");
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    doExecute(permits, lft);
    
    return lft;
  }
  
  @Override
  protected void doExecute(Runnable task) {
    doExecute(1, task);
  }
  
  /**
   * Performs the execution by scheduling the task out as necessary.  The provided permits will 
   * impact the next execution's schedule time to ensure the given rate.
   * 
   * @param permits number of permits for this task
   * @param task Runnable to be executed once rate can be maintained
   * @return Time in milliseconds task was delayed to maintain rate, or {@code -1} if rejected but handler did not throw
   */
  protected long doExecute(double permits, Runnable task) {
    double effectiveDelay = (permits / permitsPerSecond) * 1000;
    synchronized (permitLock) {
      if (permits == 0 && lastScheduleTime < Clock.lastKnownForwardProgressingMillis()) {
        // shortcut
        scheduler.execute(task);
        return 0;
      } else {
        double scheduleDelay = lastScheduleTime - Clock.accurateForwardProgressingMillis();
        if (scheduleDelay > maxScheduleDelayMillis) {
          // let rejection handler invoke outside of lock
        } else if (scheduleDelay < 1) {
          if (scheduleDelay < 0) {
            lastScheduleTime = Clock.lastKnownForwardProgressingMillis() + effectiveDelay;
          } else {
            lastScheduleTime += effectiveDelay;
          }
          scheduler.execute(task);
          return 0;
        } else {
          lastScheduleTime += effectiveDelay;
          long lScheduleDelay = (long)scheduleDelay;
          scheduler.schedule(task, lScheduleDelay);
          return lScheduleDelay;
        }
      }
    }
    
    // if not returned, then task is rejected
    rejectedExecutionHandler.handleRejectedTask(task);
    return -1;
  }
  
}
