package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SchedulerService;

/**
 * <p>This is a cross between the {@link org.threadly.concurrent.KeyDistributedScheduler} and a 
 * {@link SchedulerServiceLimiter}.  This is designed to limit concurrency for a given thread, 
 * but permit more than one thread to run at a time for a given key.  If the desired effect is to 
 * have a single thread per key, {@link org.threadly.concurrent.KeyDistributedScheduler} is a much 
 * better option.</p>
 * 
 * <p>The easiest way to use this class would be to have it distribute out schedulers through 
 * {@link #getSubmitterSchedulerForKey(Object)}.</p>
 * 
 * @deprecated Replaced by {@link org.threadly.concurrent.wrapper.limiter.KeyedSchedulerServiceLimiter}
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
@Deprecated
public class KeyedSchedulerServiceLimiter extends org.threadly.concurrent.wrapper.limiter.KeyedSchedulerServiceLimiter {
  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing only the backing scheduler 
   * and the maximum concurrency per unique key.  By default this will not rename threads for 
   * tasks executing.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency) {
    super(scheduler, maxConcurrency);
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
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName);
  }

  /**
   * Construct a new {@link KeyedSchedulerServiceLimiter} providing the backing scheduler, the 
   * maximum concurrency per unique key, and how keyed limiter threads should be named.
   * 
   * @param scheduler Scheduler to execute and schedule tasks on
   * @param maxConcurrency Maximum concurrency allowed per task key
   * @param subPoolName Name prefix for sub pools, {@code null} to not change thread names
   * @param addKeyToThreadName If {@code true} the key's .toString() will be added in the thread name
   * @param expectedTaskAdditionParallism Expected concurrent task addition access, used for performance tuning
   */
  public KeyedSchedulerServiceLimiter(SchedulerService scheduler, int maxConcurrency, 
                                      String subPoolName, boolean addKeyToThreadName, 
                                      int expectedTaskAdditionParallism) {
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, expectedTaskAdditionParallism);
  }

  /**
   * Call to check how many tasks are currently being executed in this scheduler.
   * 
   * See also: {@link SchedulerService#getCurrentRunningCount()}
   * 
   * @deprecated Please use {@link #getActiveTaskCount()} as a direct replacement.
   * 
   * @return current number of running tasks
   */
  @Deprecated
  public int getCurrentRunningCount() {
    return scheduler.getCurrentRunningCount();
  }

  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.  Because this does not lock state can be modified during the calculation of 
   * this result.  Ultimately resulting in an inaccurate number.
   * 
   * See also: {@link SchedulerService#getScheduledTaskCount()}
   * 
   * @deprecated Please use {@link #getQueuedTaskCount()} as a direct replacement.
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  @Deprecated
  public int getScheduledTaskCount() {
    return getQueuedTaskCount();
  }
}
