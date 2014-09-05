package org.threadly.concurrent;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.threadly.util.ArgumentVerifier;

/**
 * <p>This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor} to use 
 * that implementation with the {@link SubmitterSchedulerInterface}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class ScheduledExecutorServiceWrapper extends AbstractSubmitterScheduler {
  private final ScheduledExecutorService scheduler;
  
  /**
   * Constructs a new wrapper with the provided scheduler implementation.
   * 
   * @param scheduler {@link ScheduledExecutorService} implementation
   */
  public ScheduledExecutorServiceWrapper(ScheduledExecutorService scheduler) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    
    this.scheduler = scheduler;
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    scheduler.schedule(task, delayInMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    scheduler.scheduleWithFixedDelay(new ThrowableSuppressingRunnable(task), 
                                     initialDelay, recurringDelay, 
                                     TimeUnit.MILLISECONDS);
  }
}
