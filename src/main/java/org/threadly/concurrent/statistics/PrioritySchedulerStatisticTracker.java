package org.threadly.concurrent.statistics;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StatisticsUtils;

/**
 * <p>An implementation of {@link PriorityScheduler} which tracks run and usage statistics.  This 
 * is designed for testing and troubleshooting.  It has a little more overhead from the normal 
 * {@link PriorityScheduler}.</p>
 * 
 * <p>It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0 (earlier forms existed since 1.0.0)
 */
public class PrioritySchedulerStatisticTracker extends PriorityScheduler 
                                               implements StatisticExecutor {
  protected final StatsManager statsManager;
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   */
  public PrioritySchedulerStatisticTracker(int poolSize) {
    this(poolSize, DEFAULT_PRIORITY, 
         DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, DEFAULT_NEW_THREADS_DAEMON);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int poolSize, boolean useDaemonThreads) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, useDaemonThreads);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, DEFAULT_NEW_THREADS_DAEMON);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean useDaemonThreads) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, useDaemonThreads, 1000);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           ThreadFactory threadFactory) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, threadFactory, 1000);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public PrioritySchedulerStatisticTracker(int poolSize, int maxStatisticWindowSize) {
    this(poolSize, DEFAULT_PRIORITY, 
         DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, DEFAULT_NEW_THREADS_DAEMON, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public PrioritySchedulerStatisticTracker(int poolSize, boolean useDaemonThreads, 
                                           int maxStatisticWindowSize) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         useDaemonThreads, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           int maxStatisticWindowSize) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         DEFAULT_NEW_THREADS_DAEMON, maxStatisticWindowSize);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean useDaemonThreads, int maxStatisticWindowSize) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, useDaemonThreads, 
         maxStatisticWindowSize, false);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * 
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param threadFactory thread factory for producing new threads within executor
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           ThreadFactory threadFactory, 
                                           int maxStatisticWindowSize) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         threadFactory, maxStatisticWindowSize, false);
  }

  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         DEFAULT_NEW_THREADS_DAEMON, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, boolean useDaemonThreads, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         useDaemonThreads, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         DEFAULT_NEW_THREADS_DAEMON, maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, boolean useDaemonThreads, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         new ConfigurableThreadFactory(PrioritySchedulerStatisticTracker.class.getSimpleName() + "-", 
                                       true, useDaemonThreads, Thread.NORM_PRIORITY, null, null), 
         maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param threadFactory thread factory for producing new threads within executor
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           ThreadFactory threadFactory, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    super(new StatisticWorkerPool(threadFactory, poolSize, 
                                  new StatsManager(maxStatisticWindowSize, accurateTime)), 
          maxWaitForLowPriorityInMs, defaultPriority);
    
    this.statsManager = ((StatisticWorkerPool)workerPool).statsManager;
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
    List<Long> resultList = new ArrayList<Long>(statsManager.highPriorityExecutionDelay);
    resultList.addAll(statsManager.lowPriorityExecutionDelay);
    resultList.addAll(statsManager.starvablePriorityExecutionDelay);
    
    return resultList;
  }
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  This is the window 
   * used for the rolling average for {@link #getAverageExecutionDelay(TaskPriority)}.  This call 
   * allows for more complex statistics (ie looking for outliers, etc).
   * 
   * @param priority Task priority to provide samples for
   * @return list which represents execution delay samples
   */
  public List<Long> getExecutionDelaySamples(TaskPriority priority) {
    if (priority == null) {
      return getExecutionDelaySamples();
    }

    return new ArrayList<Long>(statsManager.getExecutionDelaySamples(priority));
  }

  /**
   * This reports the rolling average delay from when a task was expected to run, till when the 
   * executor actually started the task.    This will return {@code -1} if no samples have been 
   * collected yet.  This call averages over all priority types, if you want the delay for a 
   * specific priority use {@link #getAverageExecutionDelay(TaskPriority)}.
   * 
   * @return Average delay for tasks to be executed in milliseconds
   */
  @Override
  public double getAverageExecutionDelay() {
    List<Long> resultList = getExecutionDelaySamples();
    
    if (resultList.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(resultList);
  }
  
  /**
   * Gets the average delay from when the task is ready, to when it is actually executed.  This will 
   * only inspect the times for a specific priority.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return Average delay for tasks to be executed in milliseconds
   */
  public double getAverageExecutionDelay(TaskPriority priority) {
    if (priority == null) {
      return getAverageExecutionDelay();
    }
    List<Long> stats = getExecutionDelaySamples(priority);
    if (stats.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(stats);
  }

  /**
   * Gets percentile values for execution delays.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.  
   * 
   * These percentiles are across all priorities combined into the same data set.  If you want 
   * percentiles for a specific priority use 
   * {@link #getExecutionDelayPercentiles(TaskPriority, double...)}.
   * 
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution delay in milliseconds
   */
  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(double... percentiles) {
    List<Long> samples = getExecutionDelaySamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  /**
   * Gets percentile values for execution delays.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution delay in milliseconds
   */
  public Map<Double, Long> getExecutionDelayPercentiles(TaskPriority priority, 
                                                        double... percentiles) {
    List<Long> samples = getExecutionDelaySamples(priority);
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  /**
   * Get raw sample data for task run durations.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.
   * 
   * These result set includes all priorities.  If you want durations for a specific priority use 
   * {@link #getExecutionDurationSamples(TaskPriority)}.
   * 
   * @return A list of task durations in milliseconds
   */
  @Override
  public List<Long> getExecutionDurationSamples() {
    List<Long> resultList = new ArrayList<Long>(statsManager.highPriorityRunDurations);
    resultList.addAll(statsManager.lowPriorityRunDurations);
    resultList.addAll(statsManager.starvablePriorityRunDurations);
    
    return resultList;
  }

  /**
   * Get raw sample data for task run durations.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.
   * 
   * These result set includes all priorities.  If you want durations for a specific priority use 
   * {@link #getExecutionDurationSamples(TaskPriority)}.
   * 
   * @param priority Task priority to provide samples for
   * @return A list of task durations in milliseconds
   */
  public List<Long> getExecutionDurationSamples(TaskPriority priority) {
    if (priority == null) {
      return getExecutionDurationSamples();
    }
    
    return new ArrayList<Long>(statsManager.getExecutionDurationSamples(priority));
  }

  /**
   * Get the average duration that tasks submitted through this executor have spent executing.  
   * This only reports samples from tasks which have completed (in-progress tasks are not 
   * considered).  
   * 
   * This call averages over all priority types, if you want the duration for a specific priority 
   * use {@link #getAverageExecutionDuration(TaskPriority)}.
   * 
   * @return Average task execution duration in milliseconds
   */
  @Override
  public double getAverageExecutionDuration() {
    List<Long> runDurations = getExecutionDurationSamples();
    if (runDurations.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(runDurations);
  }

  /**
   * Get the average duration that tasks submitted through this executor have spent executing.  
   * This only reports samples from tasks which have completed (in-progress tasks are not 
   * considered).
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return Average task execution duration in milliseconds
   */
  public double getAverageExecutionDuration(TaskPriority priority) {
    List<Long> runDurations = getExecutionDurationSamples(priority);
    if (runDurations.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(runDurations);
  }

  /**
   * Gets percentile values for execution duration.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.  
   * 
   * These percentiles are across all priorities combined into the same data set.  If you want 
   * percentiles for a specific priority use 
   * {@link #getExecutionDurationPercentiles(TaskPriority, double...)}.
   * 
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution duration in milliseconds
   */
  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(double... percentiles) {
    List<Long> samples = getExecutionDurationSamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  /**
   * Gets percentile values for execution duration.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution duration in milliseconds
   */
  public Map<Double, Long> getExecutionDurationPercentiles(TaskPriority priority, 
                                                           double... percentiles) {
    List<Long> samples = getExecutionDurationSamples(priority);
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  @Override
  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis) {
    List<Pair<Runnable, StackTraceElement[]>> result = new ArrayList<Pair<Runnable, StackTraceElement[]>>();
    if (statsManager.accurateTime) {
      // ensure clock is updated before loop
      Clock.accurateForwardProgressingMillis();
    }
    for (Map.Entry<Pair<Thread, TaskStatWrapper>, Long> e : statsManager.runningTasks.entrySet()) {
      if (Clock.lastKnownForwardProgressingMillis() - e.getValue() > durationLimitMillis) {
        Runnable task = e.getKey().getRight().task;
        if (task instanceof ListenableFutureTask) {
          ListenableFutureTask<?> lft = (ListenableFutureTask<?>)task;
          if (lft.getContainedCallable() instanceof RunnableCallableAdapter) {
            RunnableCallableAdapter<?> rca = (RunnableCallableAdapter<?>)lft.getContainedCallable();
            task = rca.getContainedRunnable();
          }
        }
        StackTraceElement[] stack = e.getKey().getLeft().getStackTrace();
        // verify still in collection after capturing stack
        if (statsManager.runningTasks.containsKey(e.getKey())) {
          result.add(new Pair<Runnable, StackTraceElement[]>(task, stack));
        }
      }
    }
    
    return result;
  }

  @Override
  public int getLongRunningTasksQty(long durationLimitMillis) {
    int result = 0;
    
    long now = statsManager.accurateTime ? 
                 Clock.accurateForwardProgressingMillis() : 
                 Clock.lastKnownForwardProgressingMillis();
    Iterator<Long> it = statsManager.runningTasks.values().iterator();
    while (it.hasNext()) {
      Long startTime = it.next();
      if (now - startTime >= durationLimitMillis) {
        result++;
      }
    }
    
    return result;
  }
  
  @Override
  public void resetCollectedStats() {
    for (TaskPriority p : TaskPriority.values()) {
      statsManager.getExecutionDelaySamples(p).clear();
      statsManager.getExecutionDurationSamples(p).clear();
    }
  }
  
  @Override
  public long getTotalExecutionCount() {
    long result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += statsManager.getExecutionCount(p).get();
    }
    return result;
  }
  
  /**
   * Call to get the total quantity of tasks this executor has handled for a specific priority.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return total quantity of tasks run
   */
  public long getTotalExecutionCount(TaskPriority priority) {
    if (priority == null) {
      return getTotalExecutionCount();
    }
    return statsManager.getExecutionCount(priority).get();
  }
  
  /**
   * <p>This class primarily holds the structures used to store the statistics.  These can not be 
   * maintained in the parent class since sub classes need to be able to access them.  This is to 
   * help facilitate allowing the parent class to be garbage collected freely despite references 
   * to subclasses from other thread garbage collection roots.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class StatsManager {
    protected final int maxWindowSize;
    protected final boolean accurateTime;
    protected final AtomicLong totalHighPriorityExecutions;
    protected final AtomicLong totalLowPriorityExecutions;
    protected final AtomicLong totalStarvablePriorityExecutions;
    protected final ConcurrentHashMap<Pair<Thread, TaskStatWrapper>, Long> runningTasks;
    protected final ConcurrentArrayList<Long> starvablePriorityRunDurations;
    protected final ConcurrentArrayList<Long> lowPriorityRunDurations;
    protected final ConcurrentArrayList<Long> highPriorityRunDurations;
    protected final ConcurrentArrayList<Long> starvablePriorityExecutionDelay;
    protected final ConcurrentArrayList<Long> lowPriorityExecutionDelay;
    protected final ConcurrentArrayList<Long> highPriorityExecutionDelay;
    
    protected StatsManager(int maxWindowSize, boolean accurateTime) {
      this.maxWindowSize = maxWindowSize;
      this.accurateTime = accurateTime;
      totalHighPriorityExecutions = new AtomicLong(0);
      totalLowPriorityExecutions = new AtomicLong(0);
      totalStarvablePriorityExecutions = new AtomicLong(0);
      runningTasks = new ConcurrentHashMap<Pair<Thread, TaskStatWrapper>, Long>();
      starvablePriorityRunDurations = new ConcurrentArrayList<Long>(0, maxWindowSize);
      lowPriorityRunDurations = new ConcurrentArrayList<Long>(0, maxWindowSize);
      highPriorityRunDurations = new ConcurrentArrayList<Long>(0, maxWindowSize);
      starvablePriorityExecutionDelay = new ConcurrentArrayList<Long>(0, maxWindowSize);
      lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, maxWindowSize);
      highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, maxWindowSize);
    }
    
    /**
     * Get raw collection for storing execution durations.
     * 
     * @param priority TaskPriority to look up against, can not be {@code null}
     * @return Collection of execution duration statistics
     */
    protected ConcurrentArrayList<Long> getExecutionDurationSamples(TaskPriority priority) {
      switch (priority) {
        case High:
          return highPriorityRunDurations;
        case Low:
          return lowPriorityRunDurations;
        case Starvable:
          return starvablePriorityRunDurations;
        default:
          throw new UnsupportedOperationException();
      }
    }
    
    /**
     * Get raw collection for storing execution delays.
     * 
     * @param priority TaskPriority to look up against, can not be {@code null}
     * @return Collection of execution delay statistics
     */
    protected ConcurrentArrayList<Long> getExecutionDelaySamples(TaskPriority priority) {
      switch (priority) {
        case High:
          return highPriorityExecutionDelay;
        case Low:
          return lowPriorityExecutionDelay;
        case Starvable:
          return starvablePriorityExecutionDelay;
        default:
          throw new UnsupportedOperationException();
      }
    }
    
    /**
     * Get the raw atomic for storing execution counts for a given priority.
     * 
     * @param priority TaskPriority to look up against, can not be {@code null}
     * @return AtomicLong to track executions
     */
    protected AtomicLong getExecutionCount(TaskPriority priority) {
      switch (priority) {
        case High:
          return totalHighPriorityExecutions;
        case Low:
          return totalLowPriorityExecutions;
        case Starvable:
          return totalStarvablePriorityExecutions;
        default:
          throw new UnsupportedOperationException();
      }
    }

    /**
     * Called at the start of execution to track statistics around task execution.
     * 
     * @param taskPair Wrapper that is about to be executed
     */
    protected void trackTaskStart(Pair<Thread, TaskStatWrapper> taskPair) {
      getExecutionCount(taskPair.getRight().priority).incrementAndGet();
      
      runningTasks.put(taskPair, Clock.accurateForwardProgressingMillis());
    }
    
    /**
     * Used to track how long tasks are tacking to complete.
     * 
     * @param taskPair wrapper for task that completed
     */
    protected void trackTaskFinish(Pair<Thread, TaskStatWrapper> taskPair) {
      long finishTime = accurateTime ? 
                          Clock.accurateForwardProgressingMillis() : 
                          Clock.lastKnownForwardProgressingMillis();
      
      ConcurrentArrayList<Long> runDurations = getExecutionDurationSamples(taskPair.getRight().priority);
      
      Long startTime = runningTasks.remove(taskPair);
      
      synchronized (runDurations.getModificationLock()) {
        runDurations.add(finishTime - startTime);
        trimWindow(runDurations);
      }
    }
    
    /**
     * Reduces the list size to be within the max window size.
     * 
     * Should have the list synchronized/locked before calling.
     * 
     * @param list Collection to check size of and ensure is under max size
     */
    @SuppressWarnings("rawtypes")
    protected void trimWindow(Deque window) {
      while (window.size() > maxWindowSize) {
        window.removeFirst();
      }
    }
  }
  
  /**
   * <p>An extending class of {@link WorkerPool}, allowing us to gather statistics about how 
   * workers are used in the executor.  An example of such statistics are how long tasks are 
   * delayed from their desired execution.  Another example is how often a worker can be reused vs 
   * how often they have to be created.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class StatisticWorkerPool extends WorkerPool {
    protected final StatsManager statsManager;
  
    protected StatisticWorkerPool(ThreadFactory threadFactory, int poolSize, 
                                  StatsManager statsManager) {
      super(threadFactory, poolSize);
      
      this.statsManager = statsManager;
    }
    
    @Override
    public TaskWrapper workerIdle(Worker worker) {
      TaskWrapper result = super.workerIdle(worker);

      // may not be a wrapper for internal tasks like shutdown
      if (result != null && result.getContainedRunnable() instanceof TaskStatWrapper) {
        long taskDelay = Clock.lastKnownForwardProgressingMillis() - result.getPureRunTime();
        TaskStatWrapper statWrapper = (TaskStatWrapper)result.getContainedRunnable();
        ConcurrentArrayList<Long> priorityStats = 
            statsManager.getExecutionDelaySamples(statWrapper.priority);
  
        synchronized (priorityStats.getModificationLock()) {
          priorityStats.add(taskDelay);
          statsManager.trimWindow(priorityStats);
        }
      }
      
      return result;
    }
  }
  
  /**
   * <p>Wrapper for {@link Runnable} for tracking statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class TaskStatWrapper implements Runnable, RunnableContainer {
    protected final StatsManager statsManager;
    protected final TaskPriority priority;
    protected final Runnable task;
    
    public TaskStatWrapper(StatsManager statsManager, TaskPriority priority, Runnable toRun) {
      this.statsManager = statsManager;
      this.priority = priority;
      this.task = toRun;
    }
    
    @Override
    public void run() {
      Pair<Thread, TaskStatWrapper> taskPair = 
          new Pair<Thread, TaskStatWrapper>(Thread.currentThread(), this);
      statsManager.trackTaskStart(taskPair);
      try {
        task.run();
      } finally {
        statsManager.trackTaskFinish(taskPair);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
}
