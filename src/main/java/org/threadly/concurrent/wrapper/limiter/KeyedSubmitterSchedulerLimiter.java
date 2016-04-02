package org.threadly.concurrent.wrapper.limiter;

import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.ThreadRenamingSubmitterSchedulerWrapper;
import org.threadly.util.StringUtils;

/**
 * <p>This is a cross between the {@link org.threadly.concurrent.KeyDistributedScheduler} and a 
 * {@link SubmitterSchedulerLimiter}.  This is designed to limit concurrency for a given thread, 
 * but permit more than one thread to run at a time for a given key.  If the desired effect is to 
 * have a single thread per key, {@link org.threadly.concurrent.KeyDistributedScheduler} is a much 
 * better option.</p>
 * 
 * <p>The easiest way to use this class would be to have it distribute out schedulers through 
 * {@link #getSubmitterSchedulerForKey(Object)}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
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
    this(scheduler, maxConcurrency, null, false);
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
    this(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, DEFAULT_LOCK_PARALISM);
  }

  /**
   * Construct a new {@link KeyedSubmitterSchedulerLimiter} providing the backing scheduler, the 
   * maximum concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   * @param expectedTaskAdditionParallism Expected concurrent task addition access, used for performance tuning
   */
  public KeyedSubmitterSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency, 
                                        String subPoolName, boolean addKeyToThreadName, 
                                        int expectedTaskAdditionParallism) {
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, expectedTaskAdditionParallism);
  }
  
  @Override
  protected SubmitterSchedulerLimiter makeLimiter(String limiterThreadName) {
    return new SubmitterSchedulerLimiter(StringUtils.isNullOrEmpty(limiterThreadName) ? 
                                           scheduler : new ThreadRenamingSubmitterSchedulerWrapper(scheduler, 
                                                                                                   limiterThreadName, 
                                                                                                   false), 
                                         maxConcurrency);
  }
  
  /**********
   * 
   * NO IMPLEMENTATION SHOULD EXIST HERE, THIS SHOULD ALL BE IN {@link AbstractKeyedSchedulerLimiter}
   * 
   **********/
}
