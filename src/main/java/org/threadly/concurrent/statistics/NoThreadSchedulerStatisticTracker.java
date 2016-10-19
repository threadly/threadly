package org.threadly.concurrent.statistics;

import java.util.List;
import java.util.Map;

import org.threadly.concurrent.NoThreadScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.statistics.PriorityStatisticManager.TaskStatWrapper;
import org.threadly.util.Clock;
import org.threadly.util.Pair;

/**
 * An implementation of {@link NoThreadScheduler} which tracks run and usage statistics.  This is 
 * designed for testing and troubleshooting.  It has a little more overhead from the normal 
 * {@link NoThreadScheduler}.
 * 
 * It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.
 * 
 * @since 4.5.0
 */
public class NoThreadSchedulerStatisticTracker extends NoThreadScheduler 
                                               implements StatisticPriorityScheduler {
  protected final PriorityStatisticManager statsManager;
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * <p>
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   */
  public NoThreadSchedulerStatisticTracker() {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler with specified default 
   * priority behavior.  
   * <p>
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public NoThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs) {
    this(defaultPriority, maxWaitForLowPriorityInMs, 1000);
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * <p>
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public NoThreadSchedulerStatisticTracker(int maxStatisticWindowSize) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link NoThreadSchedulerStatisticTracker} scheduler.  
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public NoThreadSchedulerStatisticTracker(int maxStatisticWindowSize, boolean accurateTime) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, maxStatisticWindowSize, accurateTime);
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
    this(defaultPriority, maxWaitForLowPriorityInMs, maxStatisticWindowSize, false);
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
    super(defaultPriority, maxWaitForLowPriorityInMs);
    
    this.statsManager = new PriorityStatisticManager(maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Wraps the provided task in our statistic wrapper.  If the task is {@code null}, this will 
   * return {@code null} so that the parent class can do error checking.
   * 
   * @param task Runnable to wrap
   * @param priority Priority for runnable to execute
   * @return Runnable which is our wrapped implementation
   */
  private Runnable wrap(Runnable task, TaskPriority priority) {
    if (priority == null) {
      priority = getDefaultPriority();
    }
    if (task == null) {
      return null;
    } else {
      return new TaskStatWrapper(statsManager, priority, task);
    }
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    return super.doSchedule(new TaskStatWrapper(statsManager, priority, task), 
                            delayInMillis, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    super.scheduleWithFixedDelay(wrap(task, priority), initialDelay, recurringDelay, priority);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay,
                                  long period, TaskPriority priority) {
    super.scheduleAtFixedRate(wrap(task, priority), initialDelay, period, priority);
  }

  @Override
  public List<Long> getExecutionDelaySamples() {
    return statsManager.getExecutionDelaySamples();
  }
  
  @Override
  public List<Long> getExecutionDelaySamples(TaskPriority priority) {
    return statsManager.getExecutionDelaySamples(priority);
  }

  @Override
  public double getAverageExecutionDelay() {
    return statsManager.getAverageExecutionDelay();
  }

  @Override
  public double getAverageExecutionDelay(TaskPriority priority) {
    return statsManager.getAverageExecutionDelay(priority);
  }

  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(double... percentiles) {
    return statsManager.getExecutionDelayPercentiles(percentiles);
  }

  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(TaskPriority priority, 
                                                        double... percentiles) {
    return statsManager.getExecutionDelayPercentiles(priority, percentiles);
  }

  @Override
  public List<Long> getExecutionDurationSamples() {
    return statsManager.getExecutionDurationSamples();
  }

  @Override
  public List<Long> getExecutionDurationSamples(TaskPriority priority) {
    return statsManager.getExecutionDurationSamples(priority);
  }

  @Override
  public double getAverageExecutionDuration() {
    return statsManager.getAverageExecutionDuration();
  }

  @Override
  public double getAverageExecutionDuration(TaskPriority priority) {
    return statsManager.getAverageExecutionDuration(priority);
  }

  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(double... percentiles) {
    return statsManager.getExecutionDurationPercentiles(percentiles);
  }

  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(TaskPriority priority, 
                                                           double... percentiles) {
    return statsManager.getExecutionDurationPercentiles(priority, percentiles);
  }

  @Override
  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis) {
    return statsManager.getLongRunningTasks(durationLimitMillis);
  }

  @Override
  public int getLongRunningTasksQty(long durationLimitMillis) {
    return statsManager.getLongRunningTasksQty(durationLimitMillis);
  }
  
  @Override
  public void resetCollectedStats() {
    statsManager.resetCollectedStats();
  }
  
  @Override
  public long getTotalExecutionCount() {
    return statsManager.getTotalExecutionCount();
  }

  @Override
  public long getTotalExecutionCount(TaskPriority priority) {
    return statsManager.getTotalExecutionCount(priority);
  }
  
  @Override
  protected TaskWrapper getNextReadyTask() {
    TaskWrapper result = super.getNextReadyTask();
    
    // may not be a wrapper for internal tasks like shutdown
    if (result != null && result.getContainedRunnable() instanceof TaskStatWrapper) {
      long taskDelay = Clock.lastKnownForwardProgressingMillis() - result.getPureRunTime();
      TaskStatWrapper statWrapper = (TaskStatWrapper)result.getContainedRunnable();
      ConcurrentArrayList<Long> priorityStats = 
          statsManager.getExecutionDelaySamplesInternal(statWrapper.priority);

      synchronized (priorityStats.getModificationLock()) {
        priorityStats.add(taskDelay);
        statsManager.trimWindow(priorityStats);
      }
    }
    
    return result;
  }
}
