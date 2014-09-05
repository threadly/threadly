package org.threadly.concurrent;

/**
 * <p>This is a wrapper for {@link PriorityScheduledExecutor} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (aka the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} interface). It does make some 
 * performance sacrifices to adhere to this interface, but those are pretty minimal.  The largest 
 * compromise in here is easily scheduleAtFixedRate (which you should read the javadocs for if you 
 * need).</p>
 * 
 * @deprecated use PrioritySchedulerServiceWrapper (this class has been renamed)
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class PriorityScheduledExecutorServiceWrapper extends PrioritySchedulerServiceWrapper {
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public PriorityScheduledExecutorServiceWrapper(PriorityScheduledExecutor scheduler) {
    super(scheduler);
  }
}
