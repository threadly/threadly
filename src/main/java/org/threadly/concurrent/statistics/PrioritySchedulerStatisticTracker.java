package org.threadly.concurrent.statistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.statistics.PriorityStatisticManager.TaskStatWrapper;
import org.threadly.util.Clock;
import org.threadly.util.Pair;

/**
 * An implementation of {@link PriorityScheduler} which tracks run and usage statistics.  This is 
 * designed for testing and troubleshooting.  It has a little more overhead from the normal 
 * {@link PriorityScheduler}.
 * <p>
 * It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.
 * 
 * @since 4.5.0 (since 1.0.0 at org.threadly.concurrent)
 */
public class PrioritySchedulerStatisticTracker extends PriorityScheduler 
                                               implements StatisticPriorityScheduler {
  protected final PriorityStatisticManager statsManager;
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.  
   * <p>
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
   * <p>
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
   * <p>
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
   * <p>
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
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, useDaemonThreads, 1000, false);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.    This will by 
   * default start threads for {@link TaskPriority#Low} and higher priority tasks.
   * <p>
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
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, DEFAULT_STARVABLE_STARTS_THREADS, threadFactory);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  
   * <p>
   * This defaults to inaccurate time.  Meaning that durations and delays may under report (but 
   * NEVER OVER what they actually were).  This has the least performance impact.  If you want more 
   * accurate time consider using one of the constructors that accepts a boolean for accurate time.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param stavableStartsThreads {@code true} to have TaskPriority.Starvable tasks start new threads
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean stavableStartsThreads,  
                                           ThreadFactory threadFactory) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, stavableStartsThreads,  
         threadFactory, 1000, false);
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
   * also makes low priority tasks execution time less predictable.  This will by 
   * default start threads for {@link TaskPriority#Low} and higher priority tasks.
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
         DEFAULT_STARVABLE_STARTS_THREADS, 
         new ConfigurableThreadFactory(PrioritySchedulerStatisticTracker.class.getSimpleName() + "-", 
                                       true, useDaemonThreads, Thread.NORM_PRIORITY, null, null), 
         maxStatisticWindowSize, accurateTime);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to create a thread.  But it 
   * also makes low priority tasks execution time less predictable.  This will by 
   * default start threads for {@link TaskPriority#Low} and higher priority tasks.
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
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         DEFAULT_STARVABLE_STARTS_THREADS, threadFactory, 
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
   * @param stavableStartsThreads {@code true} to have TaskPriority.Starvable tasks start new threads
   * @param threadFactory thread factory for producing new threads within executor
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean stavableStartsThreads,
                                           ThreadFactory threadFactory, 
                                           int maxStatisticWindowSize, boolean accurateTime) {
    super(new StatisticWorkerPool(threadFactory, poolSize, stavableStartsThreads, 
                                  new PriorityStatisticManager(maxStatisticWindowSize, accurateTime)), 
          defaultPriority, maxWaitForLowPriorityInMs);
    
    this.statsManager = ((StatisticWorkerPool)workerPool).statsManager;
  }
  
  @Override
  public List<Runnable> shutdownNow() {
    // we must unwrap our statistic tracker runnables
    List<Runnable> wrappedRunnables = super.shutdownNow();
    List<Runnable> result = new ArrayList<>(wrappedRunnables.size());
    
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
    if (task == null) {
      return null;
    } else {
      return new TaskStatWrapper(statsManager, priority == null ? getDefaultPriority() : priority, task);
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
  
  /**
   * An extending class of {@link WorkerPool}, allowing us to gather statistics about how workers 
   * are used in the executor.  An example of such statistics are how long tasks are delayed from 
   * their desired execution.  Another example is how often a worker can be reused vs how often 
   * they have to be created.
   * 
   * @since 4.5.0
   */
  protected static class StatisticWorkerPool extends WorkerPool {
    protected final PriorityStatisticManager statsManager;
  
    protected StatisticWorkerPool(ThreadFactory threadFactory, int poolSize, 
                                  boolean stavableStartsThreads, 
                                  PriorityStatisticManager statsManager) {
      super(threadFactory, poolSize, stavableStartsThreads);
      
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
            statsManager.getExecutionDelaySamplesInternal(statWrapper.priority);
  
        synchronized (priorityStats.getModificationLock()) {
          priorityStats.add(taskDelay);
          statsManager.trimWindow(priorityStats);
        }
      }
      
      return result;
    }
  }
}
