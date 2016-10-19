package org.threadly.concurrent.wrapper.compatibility;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.wrapper.ThrowableSuppressingRunnable;
import org.threadly.util.ArgumentVerifier;

/**
 * This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor} to use that 
 * implementation with the {@link org.threadly.concurrent.SubmitterScheduler}.
 * 
 * @since 4.6.0 (since 1.0.0 at org.threadly.concurrent)
 */
public class ScheduledExecutorServiceWrapper extends AbstractSubmitterScheduler {
  protected final ScheduledExecutorService scheduler;
  
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
                                     initialDelay, recurringDelay, TimeUnit.MILLISECONDS);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(period, "period");
    
    scheduler.scheduleAtFixedRate(new ThrowableSuppressingRunnable(task), 
                                  initialDelay, period, TimeUnit.MILLISECONDS);
  }
}
