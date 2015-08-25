package org.threadly.concurrent;

/**
 * <p>This interface adds some more advanced features to a scheduler that are more service 
 * oriented.  Things like a concept of running/shutdown, as well as removing tasks are not always 
 * easy to implement.</p>
 * 
 * @deprecated Please use {@link SchedulerService}
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
@Deprecated
public interface SchedulerServiceInterface extends SchedulerService {
  // nothing to be removed with this deprecated interface
}
