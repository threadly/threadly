package org.threadly.concurrent.statistic;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.statistic.PriorityStatisticManager.TaskStatWrapper;
import org.threadly.util.Pair;

/**
 * <p>An implementation of {@link SingleThreadScheduler} which tracks run and usage statistics.  
 * This is designed for testing and troubleshooting.  It has a little more overhead from the normal 
 * {@link SingleThreadScheduler}.</p>
 * 
 * <p>It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0 (earlier forms existed since 1.0.0)
 */
public class SingleThreadSchedulerStatisticTracker extends SingleThreadScheduler 
                                                   implements StatisticPriorityScheduler {
  private final NoThreadSchedulerStatisticTracker statisticTracker;
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   */
  public SingleThreadSchedulerStatisticTracker() {
    this((TaskPriority)null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs) {
    this(defaultPriority, maxWaitForLowPriorityInMs, true);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread) {
    this((TaskPriority)null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, daemonThread);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               boolean daemonThread) {
    this(defaultPriority, maxWaitForLowPriorityInMs, daemonThread, 1000);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory) {
    this(defaultPriority, maxWaitForLowPriorityInMs, threadFactory, 1000);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(int maxStatisticWindowSize) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               int maxStatisticWindowSize) {
    this(defaultPriority, maxWaitForLowPriorityInMs, true, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread, int maxStatisticWindowSize) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, daemonThread, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, boolean daemonThread, 
                                               int maxStatisticWindowSize) {
    this(defaultPriority, maxWaitForLowPriorityInMs, daemonThread, maxStatisticWindowSize, false);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize) {
    this(defaultPriority, maxWaitForLowPriorityInMs, threadFactory, maxStatisticWindowSize, false);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.
   * 
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(int maxStatisticWindowSize, boolean accurateTime) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.  This defaults to using a daemon thread for the scheduler.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    this(defaultPriority, maxWaitForLowPriorityInMs, true, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(boolean daemonThread, int maxStatisticWindowSize, 
                                               boolean accurateTime) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         daemonThread, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param daemonThread {@code true} if scheduler thread should be a daemon thread
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, boolean daemonThread, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    this(defaultPriority, maxWaitForLowPriorityInMs, 
         new ConfigurableThreadFactory(SingleThreadScheduler.class.getSimpleName() + "-",
                                       true, daemonThread, Thread.NORM_PRIORITY, null, null), 
         maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, threadFactory, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new {@link SingleThreadScheduler}.  No threads will start until the first task 
   * is provided.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory factory to make thread for scheduler
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public SingleThreadSchedulerStatisticTracker(TaskPriority defaultPriority, 
                                               long maxWaitForLowPriorityInMs, 
                                               ThreadFactory threadFactory, 
                                               int maxStatisticWindowSize, boolean accurateTime) {
    super(defaultPriority, 
          new StatisticTrackerSchedulerManager(defaultPriority, 
                                               maxWaitForLowPriorityInMs, threadFactory, 
                                               maxStatisticWindowSize, accurateTime));
    
    this.statisticTracker = ((StatisticTrackerSchedulerManager)super.sManager).getStatisticTracker();
  }
  
  @Override
  public List<Runnable> shutdownNow() {
    // we must unwrap our statistic tracker runnables
    List<Runnable> wrappedRunnables = super.shutdownNow();
    List<Runnable> result = new ArrayList<Runnable>(wrappedRunnables.size());
    
    Iterator<Runnable> it = wrappedRunnables.iterator();
    while (it.hasNext()) {
      Runnable r = it.next();
      if (r instanceof TaskStatWrapper) {
        TaskStatWrapper tw = (TaskStatWrapper)r;
        if (! (tw.task instanceof Future) || ! ((Future<?>)tw.task).isCancelled()) {
          result.add(tw.task);
        }
      } else {
        // this typically happens in unit tests, but could happen by an extending class
        result.add(r);
      }
    }
    
    return result;
  }

  @Override
  public List<Long> getExecutionDelaySamples() {
    return statisticTracker.getExecutionDelaySamples();
  }
  
  @Override
  public List<Long> getExecutionDelaySamples(TaskPriority priority) {
    return statisticTracker.getExecutionDelaySamples(priority);
  }

  @Override
  public double getAverageExecutionDelay() {
    return statisticTracker.getAverageExecutionDelay();
  }

  @Override
  public double getAverageExecutionDelay(TaskPriority priority) {
    return statisticTracker.getAverageExecutionDelay(priority);
  }

  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(double... percentiles) {
    return statisticTracker.getExecutionDelayPercentiles(percentiles);
  }

  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(TaskPriority priority, 
                                                        double... percentiles) {
    return statisticTracker.getExecutionDelayPercentiles(priority, percentiles);
  }

  @Override
  public List<Long> getExecutionDurationSamples() {
    return statisticTracker.getExecutionDurationSamples();
  }

  @Override
  public List<Long> getExecutionDurationSamples(TaskPriority priority) {
    return statisticTracker.getExecutionDurationSamples(priority);
  }

  @Override
  public double getAverageExecutionDuration() {
    return statisticTracker.getAverageExecutionDuration();
  }

  @Override
  public double getAverageExecutionDuration(TaskPriority priority) {
    return statisticTracker.getAverageExecutionDuration(priority);
  }

  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(double... percentiles) {
    return statisticTracker.getExecutionDurationPercentiles(percentiles);
  }

  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(TaskPriority priority, 
                                                           double... percentiles) {
    return statisticTracker.getExecutionDurationPercentiles(priority, percentiles);
  }

  @Override
  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis) {
    return statisticTracker.getLongRunningTasks(durationLimitMillis);
  }

  @Override
  public int getLongRunningTasksQty(long durationLimitMillis) {
    return statisticTracker.getLongRunningTasksQty(durationLimitMillis);
  }
  
  @Override
  public void resetCollectedStats() {
    statisticTracker.resetCollectedStats();
  }
  
  @Override
  public long getTotalExecutionCount() {
    return statisticTracker.getTotalExecutionCount();
  }

  @Override
  public long getTotalExecutionCount(TaskPriority priority) {
    return statisticTracker.getTotalExecutionCount(priority);
  }
  
  /**
   * <p>Implementation of {@link SchedulerManager} which uses a 
   * {@link NoThreadSchedulerStatisticTracker} internally, and can be queried for a reference of 
   * this tracker.  Allowing easy access to the internal statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class StatisticTrackerSchedulerManager extends SchedulerManager {
    public StatisticTrackerSchedulerManager(TaskPriority defaultPriority, 
                                            long maxWaitForLowPriorityInMs, 
                                            ThreadFactory threadFactory, 
                                            int maxStatisticWindowSize, boolean accurateTime) {
      super(new NoThreadSchedulerStatisticTracker(defaultPriority, maxWaitForLowPriorityInMs, 
                                                  maxStatisticWindowSize, accurateTime), 
            threadFactory);
    }
    
    /**
     * Get instance of internal {@link NoThreadSchedulerStatisticTracker}.
     * 
     * @return Statistic tracker instance that is used for task management and tracking.
     */
    public NoThreadSchedulerStatisticTracker getStatisticTracker() {
      return (NoThreadSchedulerStatisticTracker)super.scheduler;
    }
  }
}
