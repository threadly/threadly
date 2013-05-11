package org.threadly.concurrent;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper for the java.util.concurrent.ScheduledThreadPoolExecutor
 * to use that implementation with the SimpleSchedulerInterface
 * 
 * @author jent - Mike Jensen
 */
public class ConcurrentSimpleSchedulerWrapper implements SimpleSchedulerInterface {
  private final ScheduledThreadPoolExecutor scheduler;
  
  /**
   * Constructs a new wrapper with the provided scheduler implementation
   * 
   * @param scheduler
   */
  public ConcurrentSimpleSchedulerWrapper(ScheduledThreadPoolExecutor scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler implementation");
    }
    
    this.scheduler = scheduler;
  }
  
  @Override
  public void execute(Runnable command) {
    scheduler.execute(command);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    scheduler.schedule(task, delayInMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduler.scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                                     TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }
}
