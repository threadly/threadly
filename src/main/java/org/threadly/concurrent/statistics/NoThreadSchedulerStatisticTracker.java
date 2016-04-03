package org.threadly.concurrent.statistics;

import org.threadly.concurrent.TaskPriority;

/**
 * <p>An implementation of {@link org.threadly.concurrent.NoThreadScheduler} which tracks run and 
 * usage statistics.  This is designed for testing and troubleshooting.  It has a little more 
 * overhead from the normal {@link org.threadly.concurrent.NoThreadScheduler}.</p>
 * 
 * <p>It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.</p>
 * 
 * @deprecated Moved to {@link org.threadly.concurrent.statistic.NoThreadSchedulerStatisticTracker}
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
@Deprecated
public class NoThreadSchedulerStatisticTracker 
                 extends org.threadly.concurrent.statistic.NoThreadSchedulerStatisticTracker 
                 implements StatisticPriorityScheduler {
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   */
  public NoThreadSchedulerStatisticTracker() {
    super();
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler with specified default 
   * priority behavior.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public NoThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs) {
    super(defaultPriority, maxWaitForLowPriorityInMs);
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public NoThreadSchedulerStatisticTracker(int maxStatisticWindowSize) {
    super(maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public NoThreadSchedulerStatisticTracker(int maxStatisticWindowSize, boolean accurateTime) {
    super(maxStatisticWindowSize, accurateTime);
  }

  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler. 
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public NoThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           int maxStatisticWindowSize) {
    super(defaultPriority, maxWaitForLowPriorityInMs, maxStatisticWindowSize);
  }

  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler, specifying all available 
   * construction options. 
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public NoThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    super(defaultPriority, maxWaitForLowPriorityInMs, maxStatisticWindowSize, accurateTime);
  }
}
