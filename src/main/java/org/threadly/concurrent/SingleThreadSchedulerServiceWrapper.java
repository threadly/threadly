package org.threadly.concurrent;

/**
 * <p>This is a wrapper for {@link SingleThreadScheduler} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (AKA the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those are 
 * pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.compatibility.SingleThreadSchedulerServiceWrapper}
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@Deprecated
public class SingleThreadSchedulerServiceWrapper 
                 extends org.threadly.concurrent.wrapper.compatibility.SingleThreadSchedulerServiceWrapper {
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public SingleThreadSchedulerServiceWrapper(SingleThreadScheduler scheduler) {
    super(scheduler);
  }
}
