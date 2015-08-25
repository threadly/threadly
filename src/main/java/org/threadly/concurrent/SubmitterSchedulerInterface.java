package org.threadly.concurrent;

/**
 * <p>A thread pool for scheduling tasks with provided futures.  This scheduler submits 
 * runnables/callables and returns futures for when they will be completed.</p>
 * 
 * @deprecated Please use {@link SubmitterScheduler}
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public interface SubmitterSchedulerInterface extends SubmitterScheduler {
  // nothing to be removed with this deprecated interface
}
