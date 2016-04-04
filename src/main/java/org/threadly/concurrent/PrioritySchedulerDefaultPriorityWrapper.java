package org.threadly.concurrent;

/**
 * <p>Class to wrap any implementation of {@link PrioritySchedulerService}.  The purpose of 
 * wrapping like this would be to change the default priority from the wrapped instance.  That way 
 * this could be passed into other parts of code and although use the same thread pool, have 
 * different default priorities.  (this could be particularly useful when used in combination with 
 * {@link KeyDistributedExecutor}, or {@link KeyDistributedScheduler}.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.wrapper.PrioritySchedulerDefaultPriorityWrapper}
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0 (since 1.0.0 as PrioritySchedulerWrapper) 
 */
@Deprecated
public class PrioritySchedulerDefaultPriorityWrapper 
                 extends org.threadly.concurrent.wrapper.PrioritySchedulerDefaultPriorityWrapper {
  /**
   * Constructs a new priority wrapper with a new default priority to use.
   * 
   * @param scheduler PriorityScheduler implementation to default to
   * @param defaultPriority default priority for tasks submitted without a priority
   */
  public PrioritySchedulerDefaultPriorityWrapper(PrioritySchedulerService scheduler, 
                                                 TaskPriority defaultPriority) {
    super(scheduler, defaultPriority);
  }
}
