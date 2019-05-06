package org.threadly.concurrent.statistics;

import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.TaskPriority;

/**
 * This interface allows for the implementor to track all executions being processed by a scheduler by passing this
 * interface to a supporting PriorityScheduler, like {@link PrioritySchedulerStatisticWriter}.
 * <p>
 * A supporting PriorityScheduler should call the {@link #trackTaskStart(TaskStatWrapper)} and {@link
 * #trackTaskFinish(TaskStatWrapper)} when execution of the task starts/finishes respectively. {@link
 * #addDelayDuration(Long, TaskPriority)} should be called when a task is removed from the queue to be executed with a
 * time duration of how long it was in the queue
 * <p>
 * Implementations of this interface need to consider the performance impact of tracking every execution and if dealing
 * with a slow service should push processing to a background thread.
 *
 * @since 5.33
 */
public interface StatisticWriter {
  
  /**
   * Called at the start of execution by the executing Thread to track statistics around task execution.
   * 
   * @param task the task about to be executed
   */
  void trackTaskStart(TaskStatWrapper task);
  
  /**
   * Called at the end of execution by the executing Thread to track how long tasks are tacking to complete.
   * 
   * @param task the task about to be executed
   */
  void trackTaskFinish(TaskStatWrapper task);
  
  /**
   * Used to track how long the task was waiting in queue to be executed
   * 
   * @param taskDelay how long the task was waiting in the queue in milliseconds
   * @param taskPriority the priority of the task waiting to be executed
   */
  void addDelayDuration(Long taskDelay, TaskPriority taskPriority);
  
  
  
  /**
   * Wrapper for {@link Runnable} for tracking statistics.
   *
   * @since 5.33
   */
  class TaskStatWrapper implements Runnable, RunnableContainer {
    protected final StatisticWriter statsManager;
    protected final TaskPriority priority;
    protected final Runnable task;
    
    public TaskStatWrapper(StatisticWriter statsManager, TaskPriority priority, Runnable toRun) {
      this.statsManager = statsManager;
      this.priority = priority;
      this.task = toRun;
    }
  
    public StatisticWriter getStatsManager() {
      return statsManager;
    }
  
    public Runnable getTask() {
      return task;
    }
  
    public TaskPriority getPriority() {
      return priority;
    }
  
    @Override
    public void run() {
      statsManager.trackTaskStart(this);
      try {
        task.run();
      } finally {
        statsManager.trackTaskFinish(this);
      }
    }
    
    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
}
