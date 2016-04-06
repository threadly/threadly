package org.threadly.concurrent.statistics;

import java.util.concurrent.Executor;

/**
 * <p>Wrap an {@link Executor} to get statistics based off executions through this wrapper.  If 
 * statistics are desired on the {@link org.threadly.concurrent.PriorityScheduler}, 
 * {@link PrioritySchedulerStatisticTracker} may be a better option, taking advantages by 
 * extending and replacing logic rather than wrapping and just adding logic.  Similarly 
 * {@link SingleThreadSchedulerStatisticTracker} and {@link NoThreadSchedulerStatisticTracker} 
 * should be used as an alternative for their respective schedulers.</p>
 * 
 * @deprecated moved to {@link org.threadly.concurrent.wrapper.statistics.ExecutorStatisticWrapper}
 *  
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
@Deprecated
public class ExecutorStatisticWrapper extends org.threadly.concurrent.wrapper.statistics.ExecutorStatisticWrapper 
                                      implements StatisticExecutor {
  /**
   * Constructs a new statistics tracker wrapper for a given executor.  This constructor uses 
   * a sensible default for the memory usage of collected statistics.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using {@link #ExecutorStatisticWrapper(Executor, boolean)}.
   * 
   * @param executor Executor to defer executions to
   */
  public ExecutorStatisticWrapper(Executor executor) {
    super(executor);
  }
  
  /**
   * Constructs a new statistics tracker wrapper for a given executor.  This constructor uses 
   * a sensible default for the memory usage of collected statistics.
   * 
   * @param executor Executor to defer executions to
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public ExecutorStatisticWrapper(Executor executor, boolean accurateTime) {
    super(executor, accurateTime);
  }

  /**
   * Constructs a new statistics tracker wrapper for a given executor.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using {@link #ExecutorStatisticWrapper(Executor, int, boolean)}.
   * 
   * @param executor Executor to defer executions to
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public ExecutorStatisticWrapper(Executor executor, int maxStatisticWindowSize) {
    super(executor, maxStatisticWindowSize);
  }

  /**
   * Constructs a new statistics tracker wrapper for a given executor.
   * 
   * @param executor Executor to defer executions to
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public ExecutorStatisticWrapper(Executor executor, 
                                  int maxStatisticWindowSize, boolean accurateTime) {
    super(executor, maxStatisticWindowSize, accurateTime);
  }
}
