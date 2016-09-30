package org.threadly.concurrent.statistics;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StatisticsUtils;

/**
 * <p>This class primarily holds the structures used to store the statistics.  These can not be 
 * maintained in the parent class since sub classes need to be able to access them.  This exists 
 * primarily to reduce code duplication between priority stats trackers (since java can only 
 * extend one class).  But it also is important as we don't want our the holding class to prevent 
 * garbage collection due to held references.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
class PriorityStatisticManager {
  protected final int maxWindowSize;
  protected final boolean accurateTime;
  protected final LongAdder totalHighPriorityExecutions;
  protected final LongAdder totalLowPriorityExecutions;
  protected final LongAdder totalStarvablePriorityExecutions;
  protected final ConcurrentHashMap<Pair<Thread, TaskStatWrapper>, Long> runningTasks;
  protected final ConcurrentArrayList<Long> starvablePriorityRunDurations;
  protected final ConcurrentArrayList<Long> lowPriorityRunDurations;
  protected final ConcurrentArrayList<Long> highPriorityRunDurations;
  protected final ConcurrentArrayList<Long> starvablePriorityExecutionDelay;
  protected final ConcurrentArrayList<Long> lowPriorityExecutionDelay;
  protected final ConcurrentArrayList<Long> highPriorityExecutionDelay;
  
  protected PriorityStatisticManager(int maxWindowSize, boolean accurateTime) {
    this.maxWindowSize = maxWindowSize;
    this.accurateTime = accurateTime;
    totalHighPriorityExecutions = new LongAdder();
    totalLowPriorityExecutions = new LongAdder();
    totalStarvablePriorityExecutions = new LongAdder();
    runningTasks = new ConcurrentHashMap<>();
    starvablePriorityRunDurations = new ConcurrentArrayList<>(0, maxWindowSize);
    lowPriorityRunDurations = new ConcurrentArrayList<>(0, maxWindowSize);
    highPriorityRunDurations = new ConcurrentArrayList<>(0, maxWindowSize);
    starvablePriorityExecutionDelay = new ConcurrentArrayList<>(0, maxWindowSize);
    lowPriorityExecutionDelay = new ConcurrentArrayList<>(0, maxWindowSize);
    highPriorityExecutionDelay = new ConcurrentArrayList<>(0, maxWindowSize);
  }
  
  /**
   * Get raw collection for storing execution durations.
   * 
   * @param priority TaskPriority to look up against, can not be {@code null}
   * @return Collection of execution duration statistics
   */
  ConcurrentArrayList<Long> getExecutionDurationSamplesInternal(TaskPriority priority) {
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
  ConcurrentArrayList<Long> getExecutionDelaySamplesInternal(TaskPriority priority) {
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
  protected LongAdder getExecutionCount(TaskPriority priority) {
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
    getExecutionCount(taskPair.getRight().priority).increment();
    
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
    
    ConcurrentArrayList<Long> runDurations = getExecutionDurationSamplesInternal(taskPair.getRight().priority);
    
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

  public List<Long> getExecutionDelaySamples() {
    List<Long> resultList = new ArrayList<>(highPriorityExecutionDelay);
    resultList.addAll(lowPriorityExecutionDelay);
    resultList.addAll(starvablePriorityExecutionDelay);
    
    return resultList;
  }
  
  public List<Long> getExecutionDelaySamples(TaskPriority priority) {
    if (priority == null) {
      return getExecutionDelaySamples();
    }

    return new ArrayList<>(getExecutionDelaySamplesInternal(priority));
  }

  public double getAverageExecutionDelay() {
    List<Long> resultList = getExecutionDelaySamples();
    
    if (resultList.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(resultList);
  }

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

  public Map<Double, Long> getExecutionDelayPercentiles(double... percentiles) {
    List<Long> samples = getExecutionDelaySamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  public Map<Double, Long> getExecutionDelayPercentiles(TaskPriority priority, 
                                                        double... percentiles) {
    List<Long> samples = getExecutionDelaySamples(priority);
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  public List<Long> getExecutionDurationSamples() {
    List<Long> resultList = new ArrayList<>(highPriorityRunDurations);
    resultList.addAll(lowPriorityRunDurations);
    resultList.addAll(starvablePriorityRunDurations);
    
    return resultList;
  }

  public List<Long> getExecutionDurationSamples(TaskPriority priority) {
    if (priority == null) {
      return getExecutionDurationSamples();
    }
    
    return new ArrayList<>(getExecutionDurationSamplesInternal(priority));
  }

  public double getAverageExecutionDuration() {
    List<Long> runDurations = getExecutionDurationSamples();
    if (runDurations.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(runDurations);
  }

  public double getAverageExecutionDuration(TaskPriority priority) {
    List<Long> runDurations = getExecutionDurationSamples(priority);
    if (runDurations.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(runDurations);
  }

  public Map<Double, Long> getExecutionDurationPercentiles(double... percentiles) {
    List<Long> samples = getExecutionDurationSamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  public Map<Double, Long> getExecutionDurationPercentiles(TaskPriority priority, 
                                                           double... percentiles) {
    List<Long> samples = getExecutionDurationSamples(priority);
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis) {
    List<Pair<Runnable, StackTraceElement[]>> result = new ArrayList<>();
    if (accurateTime) {
      // ensure clock is updated before loop
      Clock.accurateForwardProgressingMillis();
    }
    for (Map.Entry<Pair<Thread, TaskStatWrapper>, Long> e : runningTasks.entrySet()) {
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
        if (runningTasks.containsKey(e.getKey())) {
          result.add(new Pair<>(task, stack));
        }
      }
    }
    
    return result;
  }

  public int getLongRunningTasksQty(long durationLimitMillis) {
    int result = 0;
    
    long now = accurateTime ? 
                 Clock.accurateForwardProgressingMillis() : 
                 Clock.lastKnownForwardProgressingMillis();
    Iterator<Long> it = runningTasks.values().iterator();
    while (it.hasNext()) {
      Long startTime = it.next();
      if (now - startTime >= durationLimitMillis) {
        result++;
      }
    }
    
    return result;
  }
  
  public void resetCollectedStats() {
    for (TaskPriority p : TaskPriority.values()) {
      getExecutionDelaySamplesInternal(p).clear();
      getExecutionDurationSamplesInternal(p).clear();
    }
  }
  
  public long getTotalExecutionCount() {
    long result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += getExecutionCount(p).sum();
    }
    return result;
  }

  public long getTotalExecutionCount(TaskPriority priority) {
    if (priority == null) {
      return getTotalExecutionCount();
    }
    return getExecutionCount(priority).sum();
  }
  
  /**
   * <p>Wrapper for {@link Runnable} for tracking statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class TaskStatWrapper implements Runnable, RunnableContainer {
    protected final PriorityStatisticManager statsManager;
    protected final TaskPriority priority;
    protected final Runnable task;
    
    public TaskStatWrapper(PriorityStatisticManager statsManager, TaskPriority priority, Runnable toRun) {
      this.statsManager = statsManager;
      this.priority = priority;
      this.task = toRun;
    }
    
    @Override
    public void run() {
      Pair<Thread, TaskStatWrapper> taskPair = new Pair<>(Thread.currentThread(), this);
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