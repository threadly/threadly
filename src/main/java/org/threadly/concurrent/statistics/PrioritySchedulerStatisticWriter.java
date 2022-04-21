package org.threadly.concurrent.statistics;

import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.statistics.StatisticWriter.TaskStatWrapper;
import org.threadly.util.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * An implementation of {@link PriorityScheduler} which tracks run and usage statistics to the provided {@link
 * StatisticWriter}.  This is designed for testing and troubleshooting.  It has a little more overhead from the normal
 * {@link PriorityScheduler}.
 * <p>
 * It helps give insight in how long tasks are running, how well the thread pool is being utilized, as well as execution
 * frequency.
 *
 * @since 5.33
 */
public class PrioritySchedulerStatisticWriter extends PriorityScheduler{
  protected final StatisticWriter statsWriter;
  
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
   */
  public PrioritySchedulerStatisticWriter(int poolSize, StatisticWriter statsWriter) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, statsWriter);
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
  public PrioritySchedulerStatisticWriter(int poolSize, TaskPriority defaultPriority,
                                          long maxWaitForLowPriorityInMs, StatisticWriter statsWriter) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, DEFAULT_NEW_THREADS_DAEMON, statsWriter);
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
   */
  public PrioritySchedulerStatisticWriter(int poolSize, TaskPriority defaultPriority,
                                          long maxWaitForLowPriorityInMs, boolean useDaemonThreads,
                                          StatisticWriter statsWriter) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs,
         new ConfigurableThreadFactory(PrioritySchedulerStatisticWriter.class.getSimpleName() + "-",
                                       true, useDaemonThreads, Thread.NORM_PRIORITY, null, null, null),
          statsWriter);
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
   */
  public PrioritySchedulerStatisticWriter(int poolSize, TaskPriority defaultPriority,
                                          long maxWaitForLowPriorityInMs,
                                          ThreadFactory threadFactory, StatisticWriter statsWriter) {
    this(new StatisticWorkerPool(threadFactory, poolSize, false, statsWriter),
          defaultPriority, maxWaitForLowPriorityInMs);
  }
  
  protected PrioritySchedulerStatisticWriter(StatisticWorkerPool workerPool, TaskPriority defaultPriority,
                                             long maxWaitForLowPriorityInMs) {
    super(workerPool, defaultPriority, maxWaitForLowPriorityInMs);
    
    this.statsWriter = workerPool.statsWriter;
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
    if (priority == null) {
      priority = getDefaultPriority();
    }
    if (task == null) {
      return null;
    } else {
      return new TaskStatWrapper(statsWriter, priority, task);
    }
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    return super.doSchedule(new TaskStatWrapper(statsWriter, priority, task),
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

  /**
   * An extending class of {@link WorkerPool}, allowing us to gather statistics about how workers
   * are used in the executor.  An example of such statistics are how long tasks are delayed from 
   * their desired execution.  Another example is how often a worker can be reused vs how often 
   * they have to be created.
   * 
   * @since 5.33
   */
  public static class StatisticWorkerPool extends WorkerPool {
    protected final StatisticWriter statsWriter;
  
    protected StatisticWorkerPool(ThreadFactory threadFactory, int poolSize, boolean stavableStartsThreads,
                                  StatisticWriter statsWriter) {
      super(threadFactory, poolSize, stavableStartsThreads);
      
      this.statsWriter = statsWriter;
    }
    
    @Override
    public TaskWrapper workerIdle(Worker worker) {
      TaskWrapper result = super.workerIdle(worker);

      // may not be a wrapper for internal tasks like shutdown
      if (result != null && result.getContainedRunnable() instanceof TaskStatWrapper) {
        long taskDelay = Clock.lastKnownForwardProgressingMillis() - result.getPureRunTime();
        TaskStatWrapper statWrapper = (TaskStatWrapper)result.getContainedRunnable();
  
        statsWriter.addDelayDuration(taskDelay, statWrapper.priority);
      }
      
      return result;
    }
  }
}
