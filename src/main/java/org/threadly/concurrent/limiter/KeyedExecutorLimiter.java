package org.threadly.concurrent.limiter;

import java.util.concurrent.Executor;

/**
 * <p>This is a cross between the {@link org.threadly.concurrent.KeyDistributedExecutor} and an 
 * {@link ExecutorLimiter}.  This is designed to limit concurrency for a given thread, but permit 
 * more than one thread to run at a time for a given key.  If the desired effect is to have a 
 * single thread per key, {@link org.threadly.concurrent.KeyDistributedExecutor} is a much better 
 * option.</p>
 * 
 * <p>The easiest way to use this class would be to have it distribute out executors through 
 * {@link #getSubmitterExecutorForKey(Object)}.</p>
 * 
 * @deprecated moved to {@link org.threadly.concurrent.wrapper.limiter.KeyedExecutorLimiter}
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
@Deprecated
public class KeyedExecutorLimiter extends org.threadly.concurrent.wrapper.limiter.KeyedExecutorLimiter {
  /**
   * Construct a new {@link KeyedExecutorLimiter} providing only the backing executor and the 
   * maximum concurrency per unique key.  By default this will not rename threads for tasks 
   * executing.
   * 
   * @param executor Executor to execute tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency) {
    super(executor, maxConcurrency);
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
    super(executor, maxConcurrency, subPoolName, addKeyToThreadName);
  }

  /**
   * Construct a new {@link KeyedExecutorLimiter} providing the backing executor, the maximum 
   * concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param executor Executor to execute tasks on to
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   * @param expectedTaskAdditionParallism Expected concurrent task addition access, used for performance tuning
   */
  public KeyedExecutorLimiter(Executor executor, int maxConcurrency, 
                              String subPoolName, boolean addKeyToThreadName, 
                              int expectedTaskAdditionParallism) {
    super(executor, maxConcurrency, subPoolName, addKeyToThreadName, expectedTaskAdditionParallism);
  }
}
