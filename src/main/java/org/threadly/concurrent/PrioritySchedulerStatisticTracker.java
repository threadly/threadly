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
@SuppressWarnings("deprecation")  // TODO - change extends to PriorityScheduler
public class PrioritySchedulerStatisticTracker extends PriorityScheduledExecutor {
  private static final int MAX_WINDOW_SIZE = 1000;
  
  protected final AtomicInteger totalHighPriorityExecutions;
  protected final AtomicInteger totalLowPriorityExecutions;
  protected final ConcurrentHashMap<Wrapper, Long> runningTasks;
  protected final ConcurrentArrayList<Long> runTimes;
  protected final ConcurrentArrayList<Boolean> lowPriorityWorkerAvailable;
  protected final ConcurrentArrayList<Boolean> highPriorityWorkerAvailable;
  protected final ConcurrentArrayList<Long> lowPriorityExecutionDelay;
  protected final ConcurrentArrayList<Long> highPriorityExecutionDelay;
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    int endPadding = MAX_WINDOW_SIZE * 2;
    runTimes = new ConcurrentArrayList<Long>(0, endPadding);
    lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, useDaemonThreads);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    int endPadding = MAX_WINDOW_SIZE * 2;
    runTimes = new ConcurrentArrayList<Long>(0, endPadding);
    lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to make a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    int endPadding = MAX_WINDOW_SIZE * 2;
    runTimes = new ConcurrentArrayList<Long>(0, endPadding);
    lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to make a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, useDaemonThreads);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    int endPadding = MAX_WINDOW_SIZE * 2;
    runTimes = new ConcurrentArrayList<Long>(0, endPadding);
    lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.  The longer low 
   * priority tasks wait for a worker, the less chance they will have to make a thread.  But it 
   * also makes low priority tasks execution time less predictable.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, 
          threadFactory);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    int endPadding = MAX_WINDOW_SIZE * 2;
    runTimes = new ConcurrentArrayList<Long>(0, endPadding);
    lowPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    highPriorityWorkerAvailable = new ConcurrentArrayList<Boolean>(0, endPadding);
    lowPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
    highPriorityExecutionDelay = new ConcurrentArrayList<Long>(0, endPadding);
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
    runTimes.clear();
    lowPriorityWorkerAvailable.clear();
    lowPriorityExecutionDelay.clear();
    highPriorityWorkerAvailable.clear();
    highPriorityExecutionDelay.clear();
  }

  // Overridden so we can track the availability for workers for high priority tasks
  @Override
  protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (! getShutdownFinishing()) {
        synchronized (highPriorityWorkerAvailable.getModificationLock()) {
          highPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
          trimList(highPriorityWorkerAvailable);
        }
        if (getCurrentPoolSize() >= getMaxPoolSize()) {
          // we can't make the pool any bigger
          w = getExistingWorker(Long.MAX_VALUE);
        } else {
          lastHighDelay = 0;
          
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
      Clock.accurateTimeMillis(); // update clock for task to ensure it is accurate
      long executionDelay = task.getDelayEstimateInMillis();
      if (executionDelay <= 0) {  // recurring tasks will be rescheduled with a positive value already
        synchronized (highPriorityExecutionDelay.getModificationLock()) {
          highPriorityExecutionDelay.add(executionDelay * -1);
          trimList(highPriorityExecutionDelay);
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
      if (! getShutdownFinishing()) {
        // wait for high priority tasks that have been waiting longer than us if all workers are consumed
        long waitAmount;
        while (getCurrentPoolSize() >= getMaxPoolSize() && 
               availableWorkers.size() < WORKER_CONTENTION_LEVEL &&   // only care if there is worker contention
               ! getShutdownFinishing() &&
               ! highPriorityQueue.isEmpty() && // if there are no waiting high priority tasks, we don't care 
               (waitAmount = task.getDelayEstimateInMillis() - lastHighDelay) > LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS) {
          workersLock.wait(waitAmount);
          Clock.accurateTimeMillis(); // update for getDelayEstimateInMillis
        }
        // check if we should reset the high delay for future low priority tasks
        if (highPriorityQueue.isEmpty()) {
          lastHighDelay = 0;
        }
        
        if (! getShutdownFinishing()) {  // check again that we are still running
          if (getCurrentPoolSize() >= getMaxPoolSize()) {
            synchronized (lowPriorityWorkerAvailable.getModificationLock()) {
              lowPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
              trimList(lowPriorityWorkerAvailable);
            }
            w = getExistingWorker(Long.MAX_VALUE);
          } else if (getCurrentPoolSize() == 0) {
            synchronized (lowPriorityWorkerAvailable.getModificationLock()) {
              lowPriorityWorkerAvailable.add(false);
              trimList(lowPriorityWorkerAvailable);
            }
            w = makeNewWorker();
          } else {
            w = getExistingWorker(getMaxWaitForLowPriority());
            synchronized (lowPriorityWorkerAvailable.getModificationLock()) {
              lowPriorityWorkerAvailable.add(w != null);
              trimList(lowPriorityWorkerAvailable);
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
      Clock.accurateTimeMillis(); // update clock for task to ensure it is accurate
      long executionDelay = task.getDelayEstimateInMillis();
      if (executionDelay <= 0) {  // recurring tasks will be rescheduled with a positive value already
        synchronized (lowPriorityExecutionDelay.getModificationLock()) {
          lowPriorityExecutionDelay.add(executionDelay * -1);
          trimList(lowPriorityExecutionDelay);
        }
      }
      
      w.nextTask(task);
    }
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
  private Runnable wrap(Runnable task, 
                        TaskPriority priority, 
                        boolean recurring) {
    if (task == null) {
      return null;
    } else {
      return new RunnableStatWrapper(task, priority, recurring);
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
  private <T> Callable<T> wrap(Callable<T> task, 
                               TaskPriority priority, 
                               boolean recurring) {
    if (task == null) {
      return null;
    } else {
      return new CallableStatWrapper<T>(task, priority, recurring);
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
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    super.schedule(wrap(task, priority, false), 
                   delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs,
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), 
                                 result, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), 
                                 delayInMs, priority);
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
    List<Long> times = new ArrayList<Long>(runTimes);
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
    List<Long> resultList = new ArrayList<Long>(lowPriorityExecutionDelay);
    resultList.addAll(highPriorityExecutionDelay);
    
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
    List<Long> times = new ArrayList<Long>(highPriorityExecutionDelay);
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
    List<Long> times = new ArrayList<Long>(lowPriorityExecutionDelay);
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
    List<Long> result = new ArrayList<Long>(highPriorityExecutionDelay);
    
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
    List<Long> result = new ArrayList<Long>(lowPriorityExecutionDelay);
    
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
    List<Long> result = new ArrayList<Long>(runTimes);
    
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
    return totalHighPriorityExecutions.get();
  }
  
  /**
   * Call to get the total quantity of low priority tasks this executor has handled.
   * 
   * @return total quantity of low priority tasks run
   */
  public int getLowPriorityTotalExecutionCount() {
    return totalLowPriorityExecutions.get();
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
    
    long now = Clock.accurateTimeMillis();
    Iterator<Entry<Wrapper, Long>> it = runningTasks.entrySet().iterator();
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
    
    long now = Clock.accurateTimeMillis();
    Iterator<Entry<Wrapper, Long>> it = runningTasks.entrySet().iterator();
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
    
    long now = Clock.accurateTimeMillis();
    Iterator<Long> it = runningTasks.values().iterator();
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
    return runningTasks.size();
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
    List<Boolean> totalList = new ArrayList<Boolean>(lowPriorityWorkerAvailable);
    totalList.addAll(highPriorityWorkerAvailable);
      
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
    List<Boolean> list = new ArrayList<Boolean>(highPriorityWorkerAvailable);
    
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
    List<Boolean> list = new ArrayList<Boolean>(lowPriorityWorkerAvailable);
    
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
   * Called at the start of execution to track statistics around task execution.
   * 
   * @param taskWrapper Wrapper that is about to be executed
   */
  protected void trackTaskStart(Wrapper taskWrapper) {
    runningTasks.put(taskWrapper, taskWrapper.startTime = Clock.accurateTimeMillis());
    
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
    long finishTime = Clock.accurateTimeMillis();
    synchronized (runTimes.getModificationLock()) {
      runTimes.add(finishTime - taskWrapper.startTime);
      trimList(runTimes);
    }
    runningTasks.remove(taskWrapper);
  }
  
  /**
   * Reduces the list size to be within the max window size.
   * 
   * Should have the list synchronized/locked before calling.
   * 
   * @param list LinkedList to check size of.
   */
  @SuppressWarnings("rawtypes")
  protected static void trimList(Deque list) {
    while (list.size() > MAX_WINDOW_SIZE) {
      list.removeFirst();
    }
  }
  
  /**
   * <p>Wrapper for any task which needs to track statistics.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract class Wrapper {
    public final boolean callable;
    public final TaskPriority priority;
    public final boolean recurring;
    // set when trackTaskStart called, and only read from trackTaskFinish
    // so should only be accessed and used from within the same thread
    private long startTime;
    
    public Wrapper(boolean callable, 
                   TaskPriority priority, 
                   boolean recurring) {
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
  protected class RunnableStatWrapper extends Wrapper 
                                      implements Runnable, 
                                                 RunnableContainerInterface {
    private final Runnable toRun;
    
    public RunnableStatWrapper(Runnable toRun, 
                               TaskPriority priority, 
                               boolean recurring) {
      super(false, priority, recurring);
      
      this.toRun = toRun;
    }
    
    @Override
    public void run() {
      trackTaskStart(this);
      try {
        toRun.run();
      } finally {
        trackTaskFinish(this);
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
  protected class CallableStatWrapper<T> extends Wrapper 
                                         implements Callable<T>, 
                                                    CallableContainerInterface<T> {
    private final Callable<T> toRun;
    
    public CallableStatWrapper(Callable<T> toRun, 
                               TaskPriority priority, 
                               boolean recurring) {
      super(true, priority, recurring);
      
      this.toRun = toRun;
    }
    
    @Override
    public T call() throws Exception {
      trackTaskStart(this);
      try {
        return toRun.call();
      } finally {
        trackTaskFinish(this);
      }
    }

    @Override
    public Callable<T> getContainedCallable() {
      return toRun;
    }
  }
}
