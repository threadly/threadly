package org.threadly.concurrent;

import java.util.concurrent.ScheduledExecutorService;

/**
 * <p>This is a wrapper for the {@link java.util.concurrent.ScheduledThreadPoolExecutor} to use 
 * that implementation with the {@link SubmitterScheduler}.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.compatibility.ScheduledExecutorServiceWrapper}
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class ScheduledExecutorServiceWrapper 
                 extends org.threadly.concurrent.wrapper.compatibility.ScheduledExecutorServiceWrapper {
  /**
   * Constructs a new wrapper with the provided scheduler implementation.
   * 
   * @param scheduler {@link ScheduledExecutorService} implementation
   */
  public ScheduledExecutorServiceWrapper(ScheduledExecutorService scheduler) {
    super(scheduler);
  }
}
