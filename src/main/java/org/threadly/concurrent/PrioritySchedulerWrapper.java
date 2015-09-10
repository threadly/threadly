package org.threadly.concurrent;

/**
 * <p>Class to wrap any implementation of {@link PrioritySchedulerService}.  The purpose of 
 * wrapping like this would be to change the default priority from the wrapped instance.  That way 
 * this could be passed into other parts of code and although use the same thread pool, have 
 * different default priorities.  (this could be particularly useful when used in combination with 
 * {@link KeyDistributedExecutor}, or {@link KeyDistributedScheduler}.</p>
 * 
 * @deprecated Use {@link PrioritySchedulerDefaultPriorityWrapper} as a direct replacement
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class PrioritySchedulerWrapper extends PrioritySchedulerDefaultPriorityWrapper 
                                      implements PrioritySchedulerInterface {
  /**
   * Constructs a new priority wrapper with a new default priority to use.
   * 
   * @param scheduler PriorityScheduler implementation to default to
   * @param defaultPriority default priority for tasks submitted without a priority
   */
  public PrioritySchedulerWrapper(PrioritySchedulerService scheduler, 
                                  TaskPriority defaultPriority) {
    super(scheduler, defaultPriority);
  }
}