package org.threadly.concurrent.statistics;

import java.util.List;
import java.util.Map;

import org.threadly.util.Pair;

/**
 * <p>Interface for some basic statistic elements provided by any statistic executor/scheduler or 
 * any executor/scheduler wrappers.</p>
 * 
 * @author jent
 * @since 4.5.0
 */
public interface StatisticExecutor {
  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.
   * 
   * @return Number of tasks still waiting to be executed.
   */
  public int getQueuedTaskCount();
  
  /**
   * Get raw sample data for task execution delays.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.
   * 
   * @return A list of delay times in milliseconds before a task was executed
   */
  public List<Long> getExecutionDelaySamples();
  
  /**
   * This reports the rolling average delay from when a task was expected to run, till when the 
   * executor actually started the task.  This will return {@code -1} if no samples have been 
   * collected yet.
   * 
   * @return Average delay till execution in milliseconds
   */
  public double getAverageExecutionDelay();

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
  public Map<Double, Long> getExecutionDelayPercentiles(double ... percentiles);

  /**
   * Get raw sample data for task run durations.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.
   * 
   * @return A list of task durations in milliseconds
   */
  public List<Long> getExecutionDurationSamples();
  
  /**
   * Get the average duration that tasks submitted through this executor have spent executing.  
   * This only reports samples from tasks which have completed (in-progress tasks are not 
   * considered).
   * 
   * @return Average task execution duration in milliseconds
   */
  public double getAverageExecutionDuration();

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
  public Map<Double, Long> getExecutionDurationPercentiles(double ... percentiles);
  
  /**
   * Call to get a list of runnables and stack traces from tasks which have been actively 
   * executing for a longer duration than the one provided.  Time in queue waiting for execution is 
   * not considered as part of the execution duration.  
   * 
   * If only the quantity of long running tasks is needed, please use 
   * {@link #getLongRunningTasksQty(long)}.  Since it does not need to generate stack traces it is 
   * a cheaper alternative.
   * 
   * The left side of the {@link Pair} is the runnable task submitted.  If the task was submitted 
   * as a {@link java.util.concurrent.Callable} the Runnable will be of type: 
   * {@link org.threadly.concurrent.future.ListenableFutureTask}.  Casting and invoking 
   * {@link org.threadly.concurrent.future.ListenableFutureTask#getContainedCallable()} will allow 
   * you to get to your original {@link java.util.concurrent.Callable}. 
   * 
   * The right side of the {@link Pair} is a single sample of what that long running tasks stack 
   * was.  Because these tasks are running concurrently by the time this function returns the 
   * provided tasks may have completed.  
   * 
   * @param durationLimitMillis Limit for tasks execution, if task execution time is below this they will be ignored
   * @return List of long running runnables with their corresponding stack traces
   */
  public List<Pair<Runnable, StackTraceElement[]>> getLongRunningTasks(long durationLimitMillis);
  
  /**
   * Call to return the number of tasks which have been running longer than the provided duration 
   * in milliseconds.  While iterating over running tasks, it may be possible that some previously 
   * examine tasks have completed before this ran.  There is no attempt to lock tasks from starting 
   * or stopping during this check.
   * 
   * @param durationLimitMillis threshold of time in milliseconds a task must have been executing
   * @return total quantity of tasks which have or are running longer than the provided time length
   */
  public int getLongRunningTasksQty(long durationLimitMillis);
  
  /**
   * Call to get the total quantity of tasks this executor has handled.
   * 
   * @return total quantity of tasks run
   */
  public long getTotalExecutionCount();
  
  /**
   * Clears all collected rolling statistics.  These are the statistics used for averages and are 
   * limited by window sizes.  
   * 
   * This does NOT reset the total execution counts.
   */
  public void resetCollectedStats();
}
