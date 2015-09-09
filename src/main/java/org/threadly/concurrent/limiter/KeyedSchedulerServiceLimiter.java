package org.threadly.concurrent.limiter;

import java.util.concurrent.Callable;

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
 * @author jent - Mike Jensen
 * @since 4.3.0
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
    this(scheduler, maxConcurrency, null, false);
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
    this(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, DEFAULT_LOCK_PARALISM);
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
    
    this.scheduler = scheduler;
  }
  
  @Override
  protected SchedulerServiceLimiter makeLimiter(String limiterThreadName) {
    return new SchedulerServiceLimiter(scheduler, maxConcurrency, limiterThreadName);
  }

  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * 
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
   * 
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
   * 
   * See also: {@link SchedulerService#getCurrentRunningCount()}
   * 
   * @return current number of running tasks
   */
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
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount() {
    int result = 0;
    for (LimiterContainer limiter : currentLimiters.values()) {
      result += limiter.limiter.waitingTasks.size();
    }
    return result + scheduler.getScheduledTaskCount();
  }

  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * See also: {@link SchedulerService#isShutdown()}
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}
