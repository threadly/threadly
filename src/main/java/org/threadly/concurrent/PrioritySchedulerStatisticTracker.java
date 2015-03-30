package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.Clock;

/**
 * <p>An implementation of {@link PriorityScheduler} which tracks run and usage statistics.  This 
 * is designed for testing and troubleshooting.  It has a little more overhead from the normal 
 * {@link PriorityScheduler}.</p>
 * 
 * <p>It helps give insight in how long tasks are running, how well the thread pool is being 
 * utilized, as well as execution frequency.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class PrioritySchedulerStatisticTracker extends PriorityScheduler {
  protected static final int MAX_WINDOW_SIZE = 1000;
  
  protected final StatsManager statsManager;
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.
   * 
   * @param poolSize Thread pool size that should be maintained
   */
  public PrioritySchedulerStatisticTracker(int poolSize) {
    this(poolSize, DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         DEFAULT_NEW_THREADS_DAEMON);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
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
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean useDaemonThreads) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         new ConfigurableThreadFactory(PrioritySchedulerStatisticTracker.class.getSimpleName() + "-", 
                                       true, useDaemonThreads, Thread.NORM_PRIORITY, null, null));
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
  public PrioritySchedulerStatisticTracker(int poolSize, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    super(new StatisticWorkerPool(threadFactory, poolSize, maxWaitForLowPriorityInMs, new StatsManager()), 
          defaultPriority);
    
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
      if (r instanceof RunnableStatWrapper) {
        RunnableStatWrapper statWrapper = (RunnableStatWrapper)r;
        result.add(statWrapper.toRun);
      } else {
        // this typically happens in unit tests, but could happen by an extending class
        result.add(r);
      }
    }
    
    return result;
  }
  
  /**
   * Clears all collected rolling statistics.  These are the statistics used for averages and are 
   * limited by window sizes.  
   * 
   * This does NOT reset the total execution counts.
   */
  public void resetCollectedStats() {
    statsManager.runTimes.clear();
    statsManager.lowPriorityWorkerAvailable.clear();
    statsManager.lowPriorityExecutionDelay.clear();
    statsManager.highPriorityWorkerAvailable.clear();
    statsManager.highPriorityExecutionDelay.clear();
  }
  
  /**
   * Wraps the provided task in our statistic wrapper.  If the task is {@code null}, this will 
   * return {@code null} so that the parent class can do error checking.
   * 
   * @param task Runnable to wrap
   * @param priority Priority for runnable to execute
   * @param recurring {@code true} if the task is a recurring task
   * @return Runnable which is our wrapped implementation
   */
  private Runnable wrap(Runnable task, TaskPriority priority, boolean recurring) {
    if (task == null) {
      return null;
    } else {
      return new RunnableStatWrapper(statsManager, task, priority, recurring);
    }
  }
  
  /**
   * Wraps the provided task in our statistic wrapper.  If the task is {@code null}, this will 
   * return {@code null} so that the parent class can do error checking.
   * 
   * @param task Callable to wrap
   * @param priority Priority for runnable to execute
   * @param recurring {{@code true} if the task is a recurring task
   * @return Runnable which is our wrapped implementation
   */
  private <T> Callable<T> wrap(Callable<T> task, TaskPriority priority, boolean recurring) {
    if (task == null) {
      return null;
    } else {
      return new CallableStatWrapper<T>(statsManager, task, priority, recurring);
    }
  }
  
  @Override
  public void execute(Runnable task) {
    schedule(task, 0, defaultPriority);
  }
  
  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submitScheduled(task, null, 0, defaultPriority);
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return submitScheduled(task, result, 0, defaultPriority);
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return submitScheduled(task, 0, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return submitScheduled(task, result, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    super.schedule(wrap(task, priority, false), delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs,
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), delayInMs, priority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    super.scheduleWithFixedDelay(wrap(task, priority, true), 
                                 initialDelay, recurringDelay, priority);
  }
  
  /**
   * This reports the rolling average of time that tasks for this scheduler run.  It only reports 
   * for tasks which have completed.
   * 
   * @return average time in milliseconds tasks run
   */
  public long getAverageTaskRunTime() {
    return getAvgTime(getRunTimes());
  }
  
  /**
   * This reports the median run time for tasks run by this executor.  It only reports for tasks 
   * which have completed.  Returns -1 if no statistics have been collected yet.
   * 
   * @return median time in milliseconds tasks run
   */
  public long getMedianTaskRunTime() {
    List<Long> times = new ArrayList<Long>(statsManager.runTimes);
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Gets the average delay from when the task is ready, to when it is actually executed.
   * 
   * @return average delay for tasks to be executed
   */
  public long getAvgExecutionDelay() {
    List<Long> resultList = new ArrayList<Long>(statsManager.lowPriorityExecutionDelay);
    resultList.addAll(statsManager.highPriorityExecutionDelay);
    
    return getAvgTime(resultList);
  }
  
  /**
   * Gets the average delay from when the task is ready, to when it is actually executed.
   * 
   * @return average delay for high priority tasks to be executed
   */
  public long getHighPriorityAvgExecutionDelay() {
    return getAvgTime(getHighPriorityExecutionDelays());
  }
  
  /**
   * Gets the average delay from when the task is ready, to when it is actually executed.
   * 
   * @return average delay for low priority tasks to be executed
   */
  public long getLowPriorityAvgExecutionDelay() {
    return getAvgTime(getLowPriorityExecutionDelays());
  }
  
  /**
   * Gets the median delay from when the task is ready, to when it is actually executed.  Returns 
   * -1 if no statistics have been collected yet.
   * 
   * @return median delay for high priority tasks to be executed
   */
  public long getHighPriorityMedianExecutionDelay() {
    List<Long> times = new ArrayList<Long>(statsManager.highPriorityExecutionDelay);
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Gets the median delay from when the task is ready, to when it is actually executed.  Returns 
   * -1 if no statistics have been collected yet.
   * 
   * @return median delay for low priority tasks to be executed
   */
  public long getLowPriorityMedianExecutionDelay() {
    List<Long> times = new ArrayList<Long>(statsManager.lowPriorityExecutionDelay);
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  This is the window 
   * used for the rolling average for {@link #getHighPriorityAvgExecutionDelay()}.  This call 
   * allows for more complex statistics (ie looking for outliers, etc).
   * 
   * @return list which represents execution delay samples
   */
  public List<Long> getHighPriorityExecutionDelays() {
    List<Long> result = new ArrayList<Long>(statsManager.highPriorityExecutionDelay);
    
    return Collections.unmodifiableList(result);
  }
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  This is the window 
   * used for the rolling average for {@link #getLowPriorityAvgExecutionDelay()}.  This call 
   * allows for more complex statistics (ie looking for outliers, etc).
   * 
   * @return list which represents execution delay samples
   */
  public List<Long> getLowPriorityExecutionDelays() {
    List<Long> result = new ArrayList<Long>(statsManager.lowPriorityExecutionDelay);
    
    return Collections.unmodifiableList(result);
  }
  
  /**
   * Calculates the average from a collection of long values.
   * 
   * @param list List of longs to average against
   * @return -1 if the list is empty, otherwise the average of the values inside the list
   */
  private static long getAvgTime(Collection<Long> list) {
    if (list.isEmpty()) {
      return -1;
    }
    
    double totalTime = 0;
    Iterator<Long> it = list.iterator();
    while (it.hasNext()) {
      totalTime += it.next();
    }
      
    return Math.round(totalTime / list.size());
  }
  
  /**
   * Call to get a list of all currently recorded times for execution.  This is the window used 
   * for the rolling average for {@link #getAverageTaskRunTime()}.  This call allows for more 
   * complex statistics (ie looking for outliers, etc).
   * 
   * @return the list of currently recorded run times for tasks
   */
  public List<Long> getRunTimes() {
    List<Long> result = new ArrayList<Long>(statsManager.runTimes);
    
    return Collections.unmodifiableList(result);
  }
  
  /**
   * Call to get the total quantity of tasks this executor has handled.
   * 
   * @return total quantity of tasks run
   */
  public int getTotalExecutionCount() {
    return getHighPriorityTotalExecutionCount() + 
             getLowPriorityTotalExecutionCount();
  }
  
  /**
   * Call to get the total quantity of high priority tasks this executor has handled.
   * 
   * @return total quantity of high priority tasks run
   */
  public int getHighPriorityTotalExecutionCount() {
    return statsManager.totalHighPriorityExecutions.get();
  }
  
  /**
   * Call to get the total quantity of low priority tasks this executor has handled.
   * 
   * @return total quantity of low priority tasks run
   */
  public int getLowPriorityTotalExecutionCount() {
    return statsManager.totalLowPriorityExecutions.get();
  }
  
  /**
   * Call to get any {@link Runnable} that have been running longer than a given period of time.  
   * This is particularly useful when looking for runnables that may be executing longer than 
   * expected.  Cases where that happens these runnables could block the thread pool from 
   * executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for
   * @return list of runnables which are, or had been running over the provided time length
   */
  public List<Runnable> getRunnablesRunningOverTime(long timeInMs) {
    List<Runnable> result = new LinkedList<Runnable>();
    
    long now = Clock.accurateForwardProgressingMillis();
    Iterator<Entry<Wrapper, Long>> it = statsManager.runningTasks.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Wrapper, Long> entry = it.next();
      if (! entry.getKey().callable) {
        if (now - entry.getValue() >= timeInMs) {
          result.add(((RunnableStatWrapper)entry.getKey()).toRun);
        }
      }
    }
    
    return result;
  }
  
  /**
   * Call to get any {@link Callable} that have been running longer than a given period of time.  
   * This is particularly useful when looking for callables that may be executing longer than 
   * expected.  Cases where that happens these callables could block the thread pool from 
   * executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for
   * @return list of callables which are, or had been running over the provided time length
   */
  public List<Callable<?>> getCallablesRunningOverTime(long timeInMs) {
    List<Callable<?>> result = new LinkedList<Callable<?>>();
    
    long now = Clock.accurateForwardProgressingMillis();
    Iterator<Entry<Wrapper, Long>> it = statsManager.runningTasks.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Wrapper, Long> entry = it.next();
      if (entry.getKey().callable) {
        if (now - entry.getValue() >= timeInMs) {
          result.add(((CallableStatWrapper<?>)entry.getKey()).toRun);
        }
      }
    }
    
    return result;
  }
  
  /**
   * Call to return the number of callables and/or runnables which have been running longer than 
   * the provided amount of time in milliseconds.
   * 
   * @param timeInMs threshold of time to search for execution
   * @return total quantity of runnables and callables which have or are running longer than the provided time length
   */
  public int getQtyRunningOverTime(long timeInMs) {
    int result = 0;
    
    long now = Clock.accurateForwardProgressingMillis();
    Iterator<Long> it = statsManager.runningTasks.values().iterator();
    while (it.hasNext()) {
      Long startTime = it.next();
      if (now - startTime >= timeInMs) {
        result++;
      }
    }
    
    return result;
  }
  
  /* Override the implementation in PrioritySchedulerExecutor 
   * because we have the ability to have a cheaper check.
   * 
   * @see org.threadly.concurrent.PriorityScheduler#getCurrentRunningCount()
   */
  @Override
  public int getCurrentRunningCount() {
    return statsManager.runningTasks.size();
  }
  
  /**
   * Call to see how frequently tasks are able to immediately get a thread to execute on (and NOT 
   * having to create one).  
   * 
   * Returns -1 if no statistics have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused
   */
  public double getThreadAvailablePercent() {
    List<Boolean> totalList = new ArrayList<Boolean>(statsManager.lowPriorityWorkerAvailable);
    totalList.addAll(statsManager.highPriorityWorkerAvailable);
      
    return getTruePercent(totalList);
  }
  
  /**
   * Call to see how frequently high priority tasks are able to immediately get a thread to 
   * execute on (and NOT having to create one).  
   * 
   * Returns -1 if no statistics for high priority tasks have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused for high priority tasks
   */
  public double getHighPriorityThreadAvailablePercent() {
    List<Boolean> list = new ArrayList<Boolean>(statsManager.highPriorityWorkerAvailable);
    
    return getTruePercent(list);
  }
  
  /**
   * Call to see how frequently low priority tasks are able to get a thread within the max wait 
   * time for low priority tasks (and NOT having to create one).  
   * 
   * Returns -1 if no statistics for high priority tasks have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused for low priority tasks
   */
  public double getLowPriorityThreadAvailablePercent() {
    List<Boolean> list = new ArrayList<Boolean>(statsManager.lowPriorityWorkerAvailable);
    
    return getTruePercent(list);
  }
  
  /**
   * Returns the percent as a double (between 0 and 100) of how many items in the list are true, 
   * compared to the total quantity of items in the list.
   * 
   * @param list List of booleans to inspect
   * @return -1 if the list is empty, otherwise the percent of true items in the list
   */
  private static double getTruePercent(Collection<Boolean> list) {
    if (list.isEmpty()) {
      return -1;
    }
    
    double reuseCount = 0;
    Iterator<Boolean> it = list.iterator();
    while (it.hasNext()) {
      if (it.next()) {
        reuseCount++;
      }
    }
    
    return (reuseCount / list.size()) * 100;
  }
  
  /**
   * <p>This class primarily holds the structures used to store the statistics.  These can not be 
   * maintained in the parent class since sub classes need to be able to access them.  This is to 
   * help facilitate allowing the parent class to be garbage collected freely despite references 
   * to subclasses from other thread garbage collection roots.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.5.0
   */
  protected static class StatsManager {
    protected final AtomicInteger totalHighPriorityExecutions;
    protected final AtomicInteger totalLowPriorityExecutions;
    protected final ConcurrentHashMap<Wrapper, Long> runningTasks;
    protected final ConcurrentArrayList<Long> runTimes;
    protected final ConcurrentArrayList<Boolean> lowPriorityWorkerAvailable;
    protected final ConcurrentArrayList<Boolean> highPriorityWorkerAvailable;
    protected final ConcurrentArrayList<Long> lowPriorityExecutionDelay;
    protected final ConcurrentArrayList<Long> highPriorityExecutionDelay;
    
    protected StatsManager() {
      totalHighPriorityExecutions = new AtomicInteger(0);
      totalLowPriorityExecutions = new AtomicInteger(0);
      runningTasks = new ConcurrentHashMap<Wrapper, Long>();
      int endPadding = MAX_WINDOW_SIZE * 2;
      runTimes = new ConcurrentArrayList<Long>(0, endPadding);
      lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
      highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
      lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
      highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    }

    /**
     * Called at the start of execution to track statistics around task execution.
     * 
     * @param taskWrapper Wrapper that is about to be executed
     */
    protected void trackTaskStart(Wrapper taskWrapper) {
      runningTasks.put(taskWrapper, taskWrapper.startTime = Clock.accurateForwardProgressingMillis());
      
      switch (taskWrapper.priority) {
        case High:
          totalHighPriorityExecutions.incrementAndGet();
          break;
        case Low:
          totalLowPriorityExecutions.incrementAndGet();
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }
    
    /**
     * Used to track how long tasks are tacking to complete.
     * 
     * @param taskWrapper wrapper for task that completed
     */
    protected void trackTaskFinish(Wrapper taskWrapper) {
      long finishTime = Clock.accurateForwardProgressingMillis();
      synchronized (runTimes.getModificationLock()) {
        runTimes.add(finishTime - taskWrapper.startTime);
        trimWindow(runTimes);
      }
      runningTasks.remove(taskWrapper);
    }
    
    /**
     * Reduces the list size to be within the max window size.
     * 
     * Should have the list synchronized/locked before calling.
     * 
     * @param list Collection to check size of and ensure is under max size
     */
    @SuppressWarnings("rawtypes")
    protected static void trimWindow(Deque window) {
      while (window.size() > MAX_WINDOW_SIZE) {
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
   * @since 3.5.0
   */
  protected static class StatisticWorkerPool extends WorkerPool {
    protected final StatsManager statsManager;
  
    protected StatisticWorkerPool(ThreadFactory threadFactory, int poolSize, 
                                  long maxWaitForLorPriorityInMs, StatsManager statsManager) {
      super(threadFactory, poolSize, maxWaitForLorPriorityInMs);
      
      this.statsManager = statsManager;
    }

    // Overridden so we can track the availability for workers for high priority tasks
    @Override
    protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
      Worker w = null;
      synchronized (workersLock) {
        if (! isShutdownFinished()) {
          synchronized (statsManager.highPriorityWorkerAvailable.getModificationLock()) {
            statsManager.highPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
            StatsManager.trimWindow(statsManager.highPriorityWorkerAvailable);
          }
          if (getCurrentPoolSize() >= getMaxPoolSize()) {
            lastHighDelayMillis = task.getDelayEstimateInMs();
            // we can't make the pool any bigger
            w = getExistingWorker(Long.MAX_VALUE);
          } else {
            lastHighDelayMillis = 0;
            
            if (availableWorkers.isEmpty()) {
              w = makeNewWorker();
            } else {
              // always remove from the front, to get the newest worker
              w = availableWorkers.removeFirst();
            }
          }
        }
      }
      
      if (w != null) {  // may be null if shutdown
        synchronized (statsManager.highPriorityExecutionDelay.getModificationLock()) {
          Clock.systemNanoTime(); // update clock for task to ensure it is accurate
          long executionDelay = task.getDelayEstimateInMs();
          if (executionDelay <= 0) {
            statsManager.highPriorityExecutionDelay.add(executionDelay * -1);
            StatsManager.trimWindow(statsManager.highPriorityExecutionDelay);
          }
        }
        
        w.nextTask(task);
      }
    }
  
    // Overridden so we can track the availability for workers for low priority tasks
    @Override
    protected void runLowPriorityTask(TaskWrapper task) throws InterruptedException {
      Worker w = null;
      synchronized (workersLock) {
        if (! isShutdownFinished()) {
          // wait for high priority tasks that have been waiting longer than us if all workers are consumed
          long waitMs;
          while (getCurrentPoolSize() >= getMaxPoolSize() && 
                 availableWorkers.size() < WORKER_CONTENTION_LEVEL &&   // only care if there is worker contention
                 ! isShutdownFinished() &&
                 (waitMs = task.getDelayEstimateInMs() - lastHighDelayMillis) > LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS) {
            workersLock.wait(waitMs);
            Clock.systemNanoTime(); // update for getDelayEstimateInMillis
          }
          
          if (! isShutdownFinished()) {  // check again that we are still running
            if (getCurrentPoolSize() >= getMaxPoolSize()) {
              synchronized (statsManager.lowPriorityWorkerAvailable.getModificationLock()) {
                statsManager.lowPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
                StatsManager.trimWindow(statsManager.lowPriorityWorkerAvailable);
              }
              w = getExistingWorker(Long.MAX_VALUE);
            } else if (getCurrentPoolSize() == 0) {
              synchronized (statsManager.lowPriorityWorkerAvailable.getModificationLock()) {
                statsManager.lowPriorityWorkerAvailable.add(false);
                StatsManager.trimWindow(statsManager.lowPriorityWorkerAvailable);
              }
              w = makeNewWorker();
            } else {
              w = getExistingWorker(getMaxWaitForLowPriority());
              synchronized (statsManager.lowPriorityWorkerAvailable.getModificationLock()) {
                statsManager.lowPriorityWorkerAvailable.add(w != null);
                StatsManager.trimWindow(statsManager.lowPriorityWorkerAvailable);
              }
              if (w == null) {
                // this means we expired past our wait time, so create a worker if we can
                if (getCurrentPoolSize() >= getMaxPoolSize()) {
                  // more workers were created while waiting, now have reached our max
                  w = getExistingWorker(Long.MAX_VALUE);
                } else {
                  w = makeNewWorker();
                }
              }
            }
          }
        }
      }
      
      if (w != null) {  // may be null if shutdown
        synchronized (statsManager.lowPriorityExecutionDelay.getModificationLock()) {
          Clock.systemNanoTime(); // update clock for task to ensure it is accurate
          long executionDelay = task.getDelayEstimateInMs();
          if (executionDelay <= 0) {
            statsManager.lowPriorityExecutionDelay.add(executionDelay * -1);
            StatsManager.trimWindow(statsManager.lowPriorityExecutionDelay);
          }
        }
        
        w.nextTask(task);
      }
    }
  }
  
  /**
   * <p>Wrapper for any task which needs to track statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract static class Wrapper {
    public final boolean callable;
    public final TaskPriority priority;
    public final boolean recurring;
    // set when trackTaskStart called, and only read from trackTaskFinish
    // so should only be accessed and used from within the same thread
    private long startTime;
    
    public Wrapper(boolean callable, TaskPriority priority, boolean recurring) {
      this.callable = callable;
      this.priority = priority;
      this.recurring = recurring;
      startTime = -1;
    }
  }
  
  /**
   * <p>Wrapper for {@link Runnable} for tracking statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class RunnableStatWrapper extends Wrapper 
                                             implements Runnable, 
                                                        RunnableContainerInterface {
    private final StatsManager statsManager;
    private final Runnable toRun;
    
    public RunnableStatWrapper(StatsManager statsManager, Runnable toRun, 
                               TaskPriority priority, boolean recurring) {
      super(false, priority, recurring);
      
      this.statsManager = statsManager;
      this.toRun = toRun;
    }
    
    @Override
    public void run() {
      statsManager.trackTaskStart(this);
      try {
        toRun.run();
      } finally {
        statsManager.trackTaskFinish(this);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return toRun;
    }
  }

  /**
   * <p>Wrapper for {@link Callable} for tracking statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class CallableStatWrapper<T> extends Wrapper 
                                                implements Callable<T>, 
                                                           CallableContainerInterface<T> {
    private final StatsManager statsManager;
    private final Callable<T> toRun;
    
    public CallableStatWrapper(StatsManager statsManager, Callable<T> toRun, 
                               TaskPriority priority, boolean recurring) {
      super(true, priority, recurring);
      
      this.statsManager = statsManager;
      this.toRun = toRun;
    }
    
    @Override
    public T call() throws Exception {
      statsManager.trackTaskStart(this);
      try {
        return toRun.call();
      } finally {
        statsManager.trackTaskFinish(this);
      }
    }

    @Override
    public Callable<T> getContainedCallable() {
      return toRun;
    }
  }
}
