package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingSchedulerService;
import org.threadly.util.StringUtils;

/**
 * This is a cross between the {@link org.threadly.concurrent.wrapper.KeyDistributedScheduler} and 
 * a {@link SchedulerServiceLimiter}.  This is designed to limit concurrency for a given thread, 
 * but permit more than one thread to run at a time for a given key.  If the desired effect is to 
 * have a single thread per key, {@link org.threadly.concurrent.wrapper.KeyDistributedScheduler} 
 * is a much better option.
 * <p>
 * The easiest way to use this class would be to have it distribute out schedulers through 
 * {@link #getSubmitterSchedulerForKey(Object)}.
 * 
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class KeyedSchedulerServiceLimiter extends AbstractKeyedSchedulerLimiter<SchedulerServiceLimiter> {
  protected final SchedulerService scheduler;
  
  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing only the backing scheduler 
   * and the maximum concurrency per unique key.  By default this will not rename threads for 
   * tasks executing.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing only the backing scheduler 
   * and the maximum concurrency per unique key.  By default this will not rename threads for 
   * tasks executing.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency, 
                                      boolean limitFutureListenersExecution) {
    this(scheduler, maxConcurrency, null, false, limitFutureListenersExecution);
  }

  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing the backing scheduler, the maximum 
   * concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency, 
                                      String subPoolName, boolean addKeyToThreadName) {
    this(scheduler, maxConcurrency, subPoolName, addKeyToThreadName,  
         ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }

  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing the backing scheduler, the maximum 
   * concurrency per unique key, and how keyed limiter threads should be named.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency, 
                                      String subPoolName, boolean addKeyToThreadName, 
                                      boolean limitFutureListenersExecution) {
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, limitFutureListenersExecution);
    
    this.scheduler = scheduler;
  }
  
  @Override
  protected SchedulerServiceLimiter makeLimiter(String limiterThreadName) {
    return new SchedulerServiceLimiter(StringUtils.isNullOrEmpty(limiterThreadName) ? 
                                         scheduler : new ThreadRenamingSchedulerService(scheduler, 
                                                                                        limiterThreadName, 
                                                                                        false), 
                                       getMaxConcurrencyPerKey(), limitFutureListenersExecution);
  }

  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * <p>
   * See also: {@link SchedulerService#remove(Runnable)}
   * 
   * @param task The original task provided to the executor
   * @return {@code true} if the task was found and removed
   */
  public boolean remove(Runnable task) {
    for (LimiterContainer limiter : currentLimiters.values()) {
      if (limiter.limiter.remove(task)) {
        limiter.handlingTasks.decrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * <p>
   * See also: {@link SchedulerService#remove(Callable)}
   * 
   * @param task The original task provided to the executor
   * @return {@code true} if the task was found and removed
   */
  public boolean remove(Callable<?> task) {
    for (LimiterContainer limiter : currentLimiters.values()) {
      if (limiter.limiter.remove(task)) {
        limiter.handlingTasks.decrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * Call to check how many tasks are currently being executed in this scheduler.
   * <p>
   * See also: {@link SchedulerService#getActiveTaskCount()}
   * 
   * @return current number of running tasks
   */
  public int getActiveTaskCount() {
    return scheduler.getActiveTaskCount();
  }

  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.  Because this does not lock state can be modified during the calculation of 
   * this result.  Ultimately resulting in an inaccurate number.
   * <p>
   * See also: {@link SchedulerService#getQueuedTaskCount()}
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getQueuedTaskCount() {
    int result = 0;
    for (LimiterContainer limiter : currentLimiters.values()) {
      result += limiter.limiter.waitingTasks.size();
    }
    return result + scheduler.getQueuedTaskCount();
  }

  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * <p>
   * See also: {@link SchedulerService#isShutdown()}
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}
