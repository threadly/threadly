package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Executor;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingExecutor;
import org.threadly.util.StringUtils;

/**
 * This is a cross between the {@link org.threadly.concurrent.wrapper.KeyDistributedExecutor} and 
 * an {@link ExecutorLimiter}.  This is designed to limit concurrency for a given thread, but 
 * permit more than one thread to run at a time for a given key.  If the desired effect is to have 
 * a single thread per key, {@link org.threadly.concurrent.wrapper.KeyDistributedExecutor} is a 
 * much better option.
 * <p>
 * The easiest way to use this class would be to have it distribute out executors through 
 * {@link #getSubmitterExecutorForKey(Object)}.
 * 
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
public class KeyedExecutorLimiter extends AbstractKeyedLimiter<ExecutorLimiter> {
  /**
   * Construct a new {@link KeyedExecutorLimiter} providing only the backing executor and the 
   * maximum concurrency per unique key.  By default this will not rename threads for tasks 
   * executing.
   * 
   * @param executor Executor to execute tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency) {
    this(executor, maxConcurrency, ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }
  
  /**
   * Construct a new {@link KeyedExecutorLimiter} providing only the backing executor and the 
   * maximum concurrency per unique key.  By default this will not rename threads for tasks 
   * executing.
   * <p>
   * This constructor allows you to specify if listeners / 
   * {@link org.threadly.concurrent.future.FutureCallback}'s / functions in 
   * {@link ListenableFuture#map(java.util.function.Function)} or 
   * {@link ListenableFuture#flatMap(java.util.function.Function)} should be counted towards the 
   * concurrency limit.  Specifying {@code false} will release the limit as soon as the original 
   * task completes.  Specifying {@code true} will continue to enforce the limit until all listeners 
   * (without an executor) complete.
   * 
   * @param executor Executor to execute tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency,
                              boolean limitFutureListenersExecution) {
    this(executor, maxConcurrency, null, false, limitFutureListenersExecution);
  }

  /**
   * Construct a new {@link KeyedExecutorLimiter} providing the backing executor, the maximum 
   * concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param executor Executor to execute tasks on to
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency, 
                              String subPoolName, boolean addKeyToThreadName) {
    this(executor, maxConcurrency, subPoolName, addKeyToThreadName, 
         ExecutorLimiter.DEFAULT_LIMIT_FUTURE_LISTENER_EXECUTION);
  }

  /**
   * Construct a new {@link KeyedExecutorLimiter} providing the backing executor, the maximum 
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
   * @param executor Executor to execute tasks on to
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   * @param limitFutureListenersExecution {@code true} to include listener / mapped functions towards execution limit
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency, 
                              String subPoolName, boolean addKeyToThreadName,
                              boolean limitFutureListenersExecution) {
    super(executor, maxConcurrency, subPoolName, addKeyToThreadName, limitFutureListenersExecution);
  }
  
  @Override
  protected ExecutorLimiter makeLimiter(String limiterThreadName) {
    return new ExecutorLimiter(StringUtils.isNullOrEmpty(limiterThreadName) ? 
                                 executor : new ThreadRenamingExecutor(executor, limiterThreadName, false), 
                               getMaxConcurrencyPerKey(), limitFutureListenersExecution);
  }
  
  /**********
   * 
   * NO IMPLEMENTATION SHOULD EXIST HERE, THIS SHOULD ALL BE IN {@link AbstractKeyedLimiter}
   * 
   **********/
}
