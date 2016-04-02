package org.threadly.concurrent.limiter;

import org.threadly.concurrent.SimpleSchedulerInterface;

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
 * @deprecated moved to {@link org.threadly.concurrent.wrapper.limiter.RateLimiterExecutor}
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@Deprecated
public class RateLimiterExecutor extends org.threadly.concurrent.wrapper.limiter.RateLimiterExecutor {
  /**
   * Constructs a new {@link RateLimiterExecutor}.  Tasks will be scheduled on the provided 
   * scheduler, so it is assumed that the scheduler will have enough threads to handle the 
   * average permit amount per task, per second.
   * 
   * @param scheduler scheduler to schedule/execute tasks on
   * @param permitsPerSecond how many permits should be allowed per second
   */
  public RateLimiterExecutor(SimpleSchedulerInterface scheduler, double permitsPerSecond) {
    super(scheduler, permitsPerSecond);
  }
}
