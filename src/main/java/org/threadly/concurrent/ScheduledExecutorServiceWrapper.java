package org.threadly.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor}
 * to use that implementation with the {@link SubmitterSchedulerInterface}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ScheduledExecutorServiceWrapper extends AbstractSubmitterScheduler {
  private final ScheduledExecutorService scheduler;
  
  /**
   * Constructs a new wrapper with the provided scheduler implementation.
   * 
   * @param scheduler ScheduledExecutorService implementor
   */
  public ScheduledExecutorServiceWrapper(ScheduledExecutorService scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler implementation");
    }
    
    this.scheduler = scheduler;
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    scheduler.schedule(task, delayInMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    if (task == null) {
      throw new IllegalArgumentException("Runnable can not be null");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                                     TimeUnit.MILLISECONDS);
  }
}
