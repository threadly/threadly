package org.threadly.concurrent.statistics;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.StatisticsUtils;

/**
 * <p>Wrap an {@link Executor} to get statistics based off executions through this wrapper.</p>
 *  
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
public class ExecutorStatisticWrapper extends AbstractSubmitterExecutor {
  private final Executor executor;
  private final StatsContainer statsContainer;
  
  /**
   * Constructs a new statistics tracker wrapper for a given executor.  This constructor uses 
   * a sensible default for the memory usage of collected statistics.
   * 
   * @param executor Executor to defer executions to
   */
  public ExecutorStatisticWrapper(Executor executor) {
    this(executor, 1000);
  }

  /**
   * Constructs a new statistics tracker wrapper for a given executor.
   * 
   * @param executor Executor to defer executions to
   * @param maxStatisticWindowSize maximum number of samples to keep internally
   */
  public ExecutorStatisticWrapper(Executor executor, int maxStatisticWindowSize) {
    ArgumentVerifier.assertGreaterThanZero(maxStatisticWindowSize, "maxStatisticWindowSize");
    
    this.executor = executor;
    this.statsContainer = new StatsContainer(maxStatisticWindowSize);
  }
  
  @Override
  protected void doExecute(Runnable task) {
    statsContainer.queuedTaskCount.incrementAndGet();
    
    executor.execute(new StatisticRunnable(task, Clock.accurateForwardProgressingMillis(), 
                                           statsContainer));
  }
  
  /**
   * Get raw sample data for task execution delays.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link StatisticsUtils} for additional statistics.
   * 
   * @return A list of delay times in milliseconds before a task was executed
   */
  public List<Long> getExecutionDelaySamples() {
    ArrayList<Long> runDelays;
    synchronized (statsContainer.runDelays) {
      runDelays = new ArrayList<Long>(statsContainer.runDelays);
    }
    return runDelays;
  }
  
  /**
   * Get the average delay from task submission till execution time.
   * 
   * @return Average delay till execution in milliseconds
   */
  public double getAverageExecutionDelay() {
    return StatisticsUtils.getAverage(getExecutionDelaySamples());
  }

  /**
   * Gets percentile values for execution delays.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.
   * 
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution delay in milliseconds
   */
  public Map<Double, Long> getExecutionDelayPercentiles(double ... percentiles) {
    List<Long> samples = getExecutionDelaySamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }

  /**
   * Get raw sample data for task run durations.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link StatisticsUtils} for additional statistics.
   * 
   * @return A list of task durations in milliseconds
   */
  public List<Long> getExecutionDurationSamples() {
    ArrayList<Long> runDurations;
    synchronized (statsContainer.runDurations) {
      runDurations = new ArrayList<Long>(statsContainer.runDurations);
    }
    return runDurations;
  }

  /**
   * Get the average duration that tasks submitted through this wrapper have spent executing.
   * 
   * @return Average duration of task execution in milliseconds
   */
  public double getAverageExecutionDuration() {
    return StatisticsUtils.getAverage(getExecutionDurationSamples());
  }

  /**
   * Gets percentile values for execution duration.  This function accepts any decimal percentile 
   * between zero and one hundred.
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.
   * 
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested, value being the execution duration in milliseconds
   */
  public Map<Double, Long> getExecutionDurationPercentiles(double ... percentiles) {
    List<Long> samples = getExecutionDurationSamples();
    if (samples.isEmpty()) {
      samples.add(0L);
    }
    return StatisticsUtils.getPercentiles(samples, percentiles);
  }
  
  /**
   * Call to get a list of stack traces from tasks which have been actively executing for a longer 
   * duration than the one provided.  Tasks which may be possibly queued, but have not started 
   * execution during that time are not considered in this.  Each result in the provided list will 
   * be a single sample of what that long running tasks stack was.  Because these tasks are running 
   * concurrently by the time this function returns the provided tasks may have completed.  
   * 
   * If only the quantity of long running tasks is needed, please use 
   * {@link #getLongRunningTasksQty(long)}.  Since it does not need to generate stack traces it is 
   * a cheaper alternative.
   * 
   * @param durationLimitMillis Limit for tasks execution, if task execution time is below this they will be ignored
   * @return List of stack traces for long running tasks
   */
  public List<StackTraceElement[]> getLongRunningTasks(long durationLimitMillis) {
    List<StackTraceElement[]> result = new ArrayList<StackTraceElement[]>();
    
    for (Map.Entry<Thread, Long> e : statsContainer.runningTaskThreads.entrySet()) {
      if (Clock.lastKnownForwardProgressingMillis() - e.getValue() > durationLimitMillis) {
        StackTraceElement[] stack = e.getKey().getStackTrace();
        // verify still in collection after capturing stack
        if (statsContainer.runningTaskThreads.containsKey(e.getKey())) {
          result.add(stack);
        }
      }
    }
    
    return result;
  }
  
  /**
   * Call to return the number of tasks which have been running longer than the provided duration 
   * in milliseconds.  While iterating over running tasks, it may be possible that some previously 
   * examine tasks have completed before this ran.  There is no attempt to lock tasks from starting 
   * or stopping during this check.
   * 
   * @param durationLimitMillis threshold of time in milliseconds a task must have been executing
   * @return total quantity of tasks which have or are running longer than the provided time length
   */
  public int getLongRunningTasksQty(long durationLimitMillis) {
    int result = 0;
    
    for (Map.Entry<Thread, Long> e : statsContainer.runningTaskThreads.entrySet()) {
      if (Clock.lastKnownForwardProgressingMillis() - e.getValue() > durationLimitMillis) {
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
  // TODO - once we have other stats wrappers update javadoc
  public int getQueuedTaskCount() {
    return statsContainer.queuedTaskCount.get();
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
    
    public StatisticRunnable(Runnable task, long expectedRunTime, 
                             StatsContainer statsContainer) {
      this.task = task;
      this.expectedRunTime = expectedRunTime;
      this.statsContainer = statsContainer;
    }
    
    @Override
    public void run() {
      Thread t = Thread.currentThread();
      statsContainer.trackStart(t, expectedRunTime);
      try {
        task.run();
      } finally {
        statsContainer.trackFinish(t);
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
    private final int maxStatisticWindowSize;
    private final AtomicInteger queuedTaskCount;
    private final Map<Thread, Long> runningTaskThreads;
    private final Deque<Long> runDurations;
    private final Deque<Long> runDelays;
    
    public StatsContainer(int maxStatisticWindowSize) {
      this.maxStatisticWindowSize = maxStatisticWindowSize;
      this.queuedTaskCount = new AtomicInteger();
      this.runningTaskThreads = new ConcurrentHashMap<Thread, Long>();
      this.runDurations = new ArrayDeque<Long>();
      this.runDelays = new ArrayDeque<Long>();
    }
    
    public void trackStart(Thread t, long expectedRunTime) {
      // get start time before any operations for hopefully more accurate execution delay
      long startTime = Clock.accurateForwardProgressingMillis();
      
      queuedTaskCount.decrementAndGet();
      
      synchronized (runDelays) {
        runDelays.addLast(startTime - expectedRunTime);
        if (runDelays.size() > maxStatisticWindowSize) {
          runDelays.removeFirst();
        }
      }
      
      // get possibly newer time so we don't penalize stats tracking as duration
      runningTaskThreads.put(t, Clock.lastKnownForwardProgressingMillis());
    }
    
    public void trackFinish(Thread t) {
      long runDuration = Clock.lastKnownForwardProgressingMillis() - 
                           runningTaskThreads.remove(t);

      synchronized (runDurations) {
        runDurations.addLast(runDuration);
        if (runDurations.size() > maxStatisticWindowSize) {
          runDurations.removeFirst();
        }
      }
    }
  }
}
