package org.threadly.concurrent.wrapper.statistics;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.statistics.StatisticExecutor;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.Pair;
import org.threadly.util.StatisticsUtils;

/**
 * <p>Wrap an {@link Executor} to get statistics based off executions through this wrapper.  If 
 * statistics are desired on the {@link org.threadly.concurrent.PriorityScheduler}, 
 * {@link org.threadly.concurrent.statistics.PrioritySchedulerStatisticTracker} may be a better 
 * option, taking advantages by extending and replacing logic rather than wrapping and just adding 
 * logic.  Similarly 
 * {@link org.threadly.concurrent.statistics.SingleThreadSchedulerStatisticTracker} and 
 * {@link org.threadly.concurrent.statistics.NoThreadSchedulerStatisticTracker} should be used as 
 * an alternative for their respective schedulers.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 4.5.0 at org.threadly.concurrent.statistics)
 */
public class ExecutorStatisticWrapper extends AbstractSubmitterExecutor 
                                      implements StatisticExecutor {
  private final Executor executor;
  private final StatsContainer statsContainer;
  
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
    this(executor, false);
  }
  
  /**
   * Constructs a new statistics tracker wrapper for a given executor.  This constructor uses 
   * a sensible default for the memory usage of collected statistics.
   * 
   * @param executor Executor to defer executions to
   * @param accurateTime {@code true} to ensure that delays and durations are not under reported
   */
  public ExecutorStatisticWrapper(Executor executor, boolean accurateTime) {
    this(executor, 1000, accurateTime);
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
    this(executor, maxStatisticWindowSize, false);
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
    ArgumentVerifier.assertGreaterThanZero(maxStatisticWindowSize, "maxStatisticWindowSize");
    
    this.executor = executor;
    this.statsContainer = new StatsContainer(maxStatisticWindowSize, accurateTime);
  }
  
  @Override
  protected void doExecute(Runnable task) {
    statsContainer.queuedTaskCount.incrementAndGet();
    
    executor.execute(new StatisticRunnable(task, Clock.accurateForwardProgressingMillis(), 
                                           statsContainer));
  }
  
  @Override
  public List<Long> getExecutionDelaySamples() {
    ArrayList<Long> runDelays;
    synchronized (statsContainer.runDelays) {
      runDelays = new ArrayList<Long>(statsContainer.runDelays);
    }
    return runDelays;
  }
  
  @Override
  public double getAverageExecutionDelay() {
    List<Long> delaySamples = getExecutionDelaySamples();
    if (delaySamples.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(delaySamples);
  }

  @Override
  public Map<Double, Long> getExecutionDelayPercentiles(double ... percentiles) {
    List<Long> samples = getExecutionDelaySamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  @Override
  public List<Long> getExecutionDurationSamples() {
    ArrayList<Long> runDurations;
    synchronized (statsContainer.runDurations) {
      runDurations = new ArrayList<Long>(statsContainer.runDurations);
    }
    return runDurations;
  }

  @Override
  public double getAverageExecutionDuration() {
    List<Long> durationSamples = getExecutionDurationSamples();
    if (durationSamples.isEmpty()) {
      return -1;
    }
    return StatisticsUtils.getAverage(durationSamples);
  }

  @Override
  public Map<Double, Long> getExecutionDurationPercentiles(double ... percentiles) {
    List<Long> samples = getExecutionDurationSamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }
  
  @Override
  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis) {
    List<Pair<Runnable, StackTraceElement[]>> result = new ArrayList<Pair<Runnable, StackTraceElement[]>>();
    if (statsContainer.accurateTime) {
      // ensure clock is updated before loop
      Clock.accurateForwardProgressingMillis();
    }
    for (Map.Entry<Pair<Thread, Runnable>, Long> e : statsContainer.runningTasks.entrySet()) {
      if (Clock.lastKnownForwardProgressingMillis() - e.getValue() > durationLimitMillis) {
        Runnable task = e.getKey().getRight();
        if (task instanceof ListenableFutureTask) {
          ListenableFutureTask<?> lft = (ListenableFutureTask<?>)task;
          if (lft.getContainedCallable() instanceof RunnableCallableAdapter) {
            RunnableCallableAdapter<?> rca = (RunnableCallableAdapter<?>)lft.getContainedCallable();
            task = rca.getContainedRunnable();
          }
        }
        StackTraceElement[] stack = e.getKey().getLeft().getStackTrace();
        // verify still in collection after capturing stack
        if (statsContainer.runningTasks.containsKey(e.getKey())) {
          result.add(new Pair<Runnable, StackTraceElement[]>(task, stack));
        }
      }
    }
    
    return result;
  }
  
  @Override
  public int getLongRunningTasksQty(long durationLimitMillis) {
    int result = 0;

    if (statsContainer.accurateTime) {
      // ensure clock is updated before loop
      Clock.accurateForwardProgressingMillis();
    }
    for (Long l : statsContainer.runningTasks.values()) {
      if (Clock.lastKnownForwardProgressingMillis() - l > durationLimitMillis) {
        result++;
      }
    }
    
    return result;
  }
  
  /**
   * Call to check how many tasks are queued waiting for execution.  If provided an executor that 
   * allows task removal, removing tasks from that executor will cause this value to be 
   * inaccurate.  A task which is submitted, and then removed (to prevent execution), will be 
   * forever seen as queued since this wrapper has no opportunity to know about such removals.
   * 
   * @return Number of tasks still waiting to be executed.
   */
  @Override
  public int getQueuedTaskCount() {
    return statsContainer.queuedTaskCount.get();
  }

  @Override
  public long getTotalExecutionCount() {
    return statsContainer.totalExecutionCount.get();
  }
  
  @Override
  public void resetCollectedStats() {
    synchronized (statsContainer.runDelays) {
      statsContainer.runDelays.clear();
    }
    synchronized (statsContainer.runDurations) {
      statsContainer.runDurations.clear();
    }
  }
  
  /**
   * <p>Runnable wrapper that will track task execution statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class StatisticRunnable implements RunnableContainer, Runnable {
    private final Runnable task;
    private final long expectedRunTime;
    private final StatsContainer statsContainer;
    
    public StatisticRunnable(Runnable task, long expectedRunTime, StatsContainer statsContainer) {
      this.task = task;
      this.expectedRunTime = expectedRunTime;
      this.statsContainer = statsContainer;
    }
    
    @Override
    public void run() {
      Pair<Thread, Runnable> taskPair = new Pair<Thread, Runnable>(Thread.currentThread(), task);
      statsContainer.trackStart(taskPair, expectedRunTime);
      try {
        task.run();
      } finally {
        statsContainer.trackFinish(taskPair);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Class which contains and maintains the statistics collected by a statistic tracker.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  protected static class StatsContainer {
    public final int maxStatisticWindowSize;
    public final boolean accurateTime;
    protected final AtomicLong totalExecutionCount;
    protected final AtomicInteger queuedTaskCount;
    protected final Map<Pair<Thread, Runnable>, Long> runningTasks;
    protected final Deque<Long> runDurations;
    protected final Deque<Long> runDelays;
    
    public StatsContainer(int maxStatisticWindowSize, boolean accurateTime) {
      this.maxStatisticWindowSize = maxStatisticWindowSize;
      this.accurateTime = accurateTime;
      this.totalExecutionCount = new AtomicLong();
      this.queuedTaskCount = new AtomicInteger();
      this.runningTasks = new ConcurrentHashMap<Pair<Thread, Runnable>, Long>();
      this.runDurations = new ArrayDeque<Long>();
      this.runDelays = new ArrayDeque<Long>();
    }
    
    public void trackStart(Pair<Thread, Runnable> taskPair, long expectedRunTime) {
      // get start time before any operations for hopefully more accurate execution delay
      long startTime = Clock.accurateForwardProgressingMillis();
      
      queuedTaskCount.decrementAndGet();
      totalExecutionCount.incrementAndGet();
      
      synchronized (runDelays) {
        runDelays.addLast(startTime - expectedRunTime);
        if (runDelays.size() > maxStatisticWindowSize) {
          runDelays.removeFirst();
        }
      }
      
      // get possibly newer time so we don't penalize stats tracking as duration
      runningTasks.put(taskPair, Clock.lastKnownForwardProgressingMillis());
    }
    
    public void trackFinish(Pair<Thread, Runnable> taskPair) {
      long runDuration = (accurateTime ? 
                           Clock.accurateForwardProgressingMillis() : Clock.lastKnownForwardProgressingMillis()) - 
                           runningTasks.remove(taskPair);

      synchronized (runDurations) {
        runDurations.addLast(runDuration);
        if (runDurations.size() > maxStatisticWindowSize) {
          runDurations.removeFirst();
        }
      }
    }
  }
}
