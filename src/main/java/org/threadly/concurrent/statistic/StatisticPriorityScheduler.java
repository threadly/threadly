package org.threadly.concurrent.statistic;

import java.util.List;
import java.util.Map;

import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.TaskPriority;

/**
 * <p>An extension of {@link StatisticExecutor}, defining specific behavior when the statistic 
 * tracker is implementing for a scheduler which has a concept of task priorities.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
public interface StatisticPriorityScheduler extends StatisticExecutor, PrioritySchedulerService {
  /**
   * Get raw sample data for task execution delays.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.  
   * 
   * The returned result set includes all priorities.  If you want durations for a specific 
   * priority use {@link #getExecutionDelaySamples(TaskPriority)}.
   * 
   * @return A list of delay times in milliseconds before a task was executed
   */
  @Override
  public List<Long> getExecutionDelaySamples();
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  This is the window 
   * used for the rolling average for {@link #getAverageExecutionDelay(TaskPriority)}.  This call 
   * allows for more complex statistics (ie looking for outliers, etc).
   * 
   * @param priority Task priority to provide samples for
   * @return list which represents execution delay samples
   */
  public List<Long> getExecutionDelaySamples(TaskPriority priority);

  /**
   * This reports the rolling average delay from when a task was expected to run, till when the 
   * executor actually started the task.    This will return {@code -1} if no samples have been 
   * collected yet.  This call averages over all priority types, if you want the delay for a 
   * specific priority use {@link #getAverageExecutionDelay(TaskPriority)}.
   * 
   * @return Average delay for tasks to be executed in milliseconds
   */
  @Override
  public double getAverageExecutionDelay();
  
  /**
   * Gets the average delay from when the task is ready, to when it is actually executed.  This will 
   * only inspect the times for a specific priority.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return Average delay for tasks to be executed in milliseconds
   */
  public double getAverageExecutionDelay(TaskPriority priority);

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
  public Map<Double, Long> getExecutionDelayPercentiles(double... percentiles);

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
                                                        double... percentiles);

  /**
   * Get raw sample data for task run durations.  This raw data can be used for more advanced 
   * statistics which are not provided in this library.  These can also be fed into utilities in 
   * {@link org.threadly.util.StatisticsUtils} for additional statistics.  
   * 
   * The returned result set includes all priorities.  If you want durations for a specific 
   * priority use {@link #getExecutionDurationSamples(TaskPriority)}.
   * 
   * @return A list of task durations in milliseconds
   */
  @Override
  public List<Long> getExecutionDurationSamples();

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
  public List<Long> getExecutionDurationSamples(TaskPriority priority);

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
  public double getAverageExecutionDuration();

  /**
   * Get the average duration that tasks submitted through this executor have spent executing.  
   * This only reports samples from tasks which have completed (in-progress tasks are not 
   * considered).
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return Average task execution duration in milliseconds
   */
  public double getAverageExecutionDuration(TaskPriority priority);

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
  public Map<Double, Long> getExecutionDurationPercentiles(double... percentiles);

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
                                                           double... percentiles);
  
  /**
   * Call to get the total quantity of tasks this executor has handled for a specific priority.
   * 
   * @param priority Specific task priority which statistics should be calculated against
   * @return total quantity of tasks run
   */
  public long getTotalExecutionCount(TaskPriority priority);
}
