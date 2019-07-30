package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingSubmitterScheduler;
import org.threadly.util.StringUtils;

/**
 * This is a cross between the {@link org.threadly.concurrent.wrapper.KeyDistributedScheduler} and 
 * a {@link SubmitterSchedulerLimiter}.  This is designed to limit concurrency for a given 
 * thread, but permit more than one thread to run at a time for a given key.  If the desired 
 * effect is to have a single thread per key, 
 * {@link org.threadly.concurrent.wrapper.KeyDistributedScheduler} is a much better option.
 * <p>
 * The easiest way to use this class would be to have it distribute out schedulers through 
 * {@link #getSubmitterSchedulerForKey(Object)}.
 * 
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class KeyedSubmitterSchedulerLimiter extends AbstractKeyedSchedulerLimiter<SubmitterSchedulerLimiter> {
  /**
   * Construct a new {@link KeyedSubmitterSchedulerLimiter} providing only the backing scheduler 
   * and the maximum concurrency per unique key.  By default this will not rename threads for 
   * tasks executing.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   */
  public KeyedSubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency) {
    this(scheduler, maxConcurrency, ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Construct a new {@link KeyedSubmitterSchedulerLimiter} providing only the backing scheduler 
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
  public KeyedSubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency,
                                        boolean limitFutureListenersExecution) {
    this(scheduler, maxConcurrency, null, false, limitFutureListenersExecution);
  }

  /**
   * Construct a new {@link KeyedSubmitterSchedulerLimiter} providing the backing scheduler, the maximum 
   * concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   */
  public KeyedSubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency, 
                                        String subPoolName, boolean addKeyToThreadName) {
    this(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, 
         ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }

  /**
   * Construct a new {@link KeyedSubmitterSchedulerLimiter} providing the backing scheduler, the maximum 
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
  public KeyedSubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency, 
                                        String subPoolName, boolean addKeyToThreadName,
                                        boolean limitFutureListenersExecution) {
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, limitFutureListenersExecution);
  }
  
  @Override
  protected SubmitterSchedulerLimiter makeLimiter(String limiterThreadName) {
    return new SubmitterSchedulerLimiter(StringUtils.isNullOrEmpty(limiterThreadName) ? 
                                           scheduler : new ThreadRenamingSubmitterScheduler(scheduler, 
                                                                                            limiterThreadName, 
                                                                                            false), 
                                         getMaxConcurrencyPerKey(), limitFutureListenersExecution);
  }
  
  /**********
   * 
   * NO IMPLEMENTATION SHOULD EXIST HERE, THIS SHOULD ALL BE IN {@link AbstractKeyedSchedulerLimiter}
   * 
   **********/
}
