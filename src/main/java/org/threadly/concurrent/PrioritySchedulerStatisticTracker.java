package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.Clock;

/**
 * An implementation of {@link PriorityScheduledExecutor} which tracks run and usage 
 * statistics.  This is designed for testing and troubleshooting.  It has a little 
 * more overhead from the normal {@link PriorityScheduledExecutor}.
 * 
 * It helps give insight in how long tasks are running, how well the thread pool is 
 * being utilized, as well as execution frequency.
 * 
 * @author jent - Mike Jensen
 */
public class PrioritySchedulerStatisticTracker extends PriorityScheduledExecutor {
  private static final int MAX_WINDOW_SIZE = 1000;
  
  protected final AtomicInteger totalHighPriorityExecutions;
  protected final AtomicInteger totalLowPriorityExecutions;
  protected final ConcurrentHashMap<Wrapper, Long> runningTasks;
  protected final LinkedList<Long> runTimes;
  protected final LinkedList<Boolean> lowPriorityWorkerAvailable;
  protected final LinkedList<Boolean> highPriorityWorkerAvailable;
  protected final List<Long> lowPriorityExecutionDelay;
  protected final List<Long> highPriorityExecutionDelay;
  
  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This constructs a default 
   * priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  
   * defaults to all newly created threads being daemon threads.
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
    runTimes = new LinkedList<Long>();
    lowPriorityWorkerAvailable = new LinkedList<Boolean>();
    highPriorityWorkerAvailable = new LinkedList<Boolean>();
    lowPriorityExecutionDelay = new LinkedList<Long>();
    highPriorityExecutionDelay = new LinkedList<Long>();
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This constructs a default 
   * priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param useDaemonThreads boolean for if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, useDaemonThreads);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
    lowPriorityWorkerAvailable = new LinkedList<Boolean>();
    highPriorityWorkerAvailable = new LinkedList<Boolean>();
    lowPriorityExecutionDelay = new LinkedList<Long>();
    highPriorityExecutionDelay = new LinkedList<Long>();
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This provides the extra
   * parameters to tune what tasks submitted without a priority will be 
   * scheduled as.  As well as the maximum wait for low priority tasks.
   * The longer low priority tasks wait for a worker, the less chance they will
   * have to make a thread.  But it also makes low priority tasks execution time
   * less predictable.
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
    runTimes = new LinkedList<Long>();
    lowPriorityWorkerAvailable = new LinkedList<Boolean>();
    highPriorityWorkerAvailable = new LinkedList<Boolean>();
    lowPriorityExecutionDelay = new LinkedList<Long>();
    highPriorityExecutionDelay = new LinkedList<Long>();
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This provides the extra
   * parameters to tune what tasks submitted without a priority will be 
   * scheduled as.  As well as the maximum wait for low priority tasks.
   * The longer low priority tasks wait for a worker, the less chance they will
   * have to make a thread.  But it also makes low priority tasks execution time
   * less predictable.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks wait for a worker
   * @param useDaemonThreads boolean for if newly created threads should be daemon
   */
  public PrioritySchedulerStatisticTracker(int corePoolSize, int maxPoolSize,
                                           long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                           long maxWaitForLowPriorityInMs, 
                                           final boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs);
    
    totalHighPriorityExecutions = new AtomicInteger();
    totalLowPriorityExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
    lowPriorityWorkerAvailable = new LinkedList<Boolean>();
    highPriorityWorkerAvailable = new LinkedList<Boolean>();
    lowPriorityExecutionDelay = new LinkedList<Long>();
    highPriorityExecutionDelay = new LinkedList<Long>();
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This provides the extra
   * parameters to tune what tasks submitted without a priority will be 
   * scheduled as.  As well as the maximum wait for low priority tasks.
   * The longer low priority tasks wait for a worker, the less chance they will
   * have to make a thread.  But it also makes low priority tasks execution time
   * less predictable.
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
    runTimes = new LinkedList<Long>();
    lowPriorityWorkerAvailable = new LinkedList<Boolean>();
    highPriorityWorkerAvailable = new LinkedList<Boolean>();
    lowPriorityExecutionDelay = new LinkedList<Long>();
    highPriorityExecutionDelay = new LinkedList<Long>();
  }

  @Override
  protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (! isShutdown()) {
        synchronized (highPriorityWorkerAvailable) {
          highPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
          trimList(highPriorityWorkerAvailable);
        }
        if (getCurrentPoolSize() >= getMaxPoolSize()) {
          // we can't make the pool any bigger
          w = getExistingWorker(Long.MAX_VALUE);
        } else {
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
      Clock.accurateTime(); // update clock for task to ensure it is accurate
      long executionDelay = Math.abs(task.getDelay(TimeUnit.MILLISECONDS));
      highPriorityExecutionDelay.add(executionDelay);
      
      w.nextTask(task);
    }
  }
  
  @Override
  protected void runLowPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (! isShutdown()) {
        if (getCurrentPoolSize() >= getMaxPoolSize()) {
          synchronized (lowPriorityWorkerAvailable) {
            lowPriorityWorkerAvailable.add(! availableWorkers.isEmpty());
            trimList(lowPriorityWorkerAvailable);
          }
          w = getExistingWorker(Long.MAX_VALUE);
        } else {
          w = getExistingWorker(getMaxWaitForLowPriority());
          synchronized (lowPriorityWorkerAvailable) {
            lowPriorityWorkerAvailable.add(w != null);
            trimList(lowPriorityWorkerAvailable);
          }
        }
        if (w == null) {
          // this means we expired past our wait time, so just make a new worker
          if (getCurrentPoolSize() >= getMaxPoolSize()) {
            // more workers were created while waiting, now have exceeded our max
            w = getExistingWorker(Long.MAX_VALUE);
          } else {
            w = makeNewWorker();
          }
        }
      }
    }
    
    if (w != null) {  // may be null if shutdown
      Clock.accurateTime(); // update clock for task to ensure it is accurate
      long executionDelay = Math.abs(task.getDelay(TimeUnit.MILLISECONDS));
      lowPriorityExecutionDelay.add(executionDelay);
      
      w.nextTask(task);
    }
  }
  
  private Runnable wrap(Runnable task, 
                        TaskPriority priority, 
                        boolean recurring) {
    if (task == null) {
      return null;
    } else {
      return new RunnableStatWrapper(task, priority, recurring);
    }
  }
  
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
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    super.schedule(wrap(task, priority, false), 
                   delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs,
                                             TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), 
                                 delayInMs, priority);
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
   * This reports the rolling average of time that tasks for this 
   * scheduler run.  It only reports for tasks which have completed.
   * 
   * @return average time in milliseconds tasks run
   */
  public long getAverageTaskRunTime() {
    return getAvgTime(getRunTimes());
  }
  
  /**
   * This reports the median run time for tasks run by this executor.  
   * It only reports for tasks which have completed.  Returns -1 if 
   * no stats have been collected yet.
   * 
   * @return median time in milliseconds tasks run
   */
  public long getMedianTaskRunTime() {
    List<Long> times;
    synchronized (runTimes) {
      times = new ArrayList<Long>(runTimes);
    }
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Gets the average delay from when the task is ready, to when 
   * it is actually executed.
   * 
   * @return average delay for tasks to be executed
   */
  public long getAvgExecutionDelay() {
    List<Long> resultList = new LinkedList<Long>();
    synchronized (lowPriorityExecutionDelay) {
      resultList.addAll(lowPriorityExecutionDelay);
    }
    synchronized (highPriorityExecutionDelay) {
      resultList.addAll(highPriorityExecutionDelay);
    }
    
    return getAvgTime(resultList);
  }
  
  /**
   * Gets the average delay from when the task is ready, to when 
   * it is actually executed.
   * 
   * @return average delay for high priority tasks to be executed
   */
  public long getHighPriorityAvgExecutionDelay() {
    return getAvgTime(getHighPriorityExecutionDelays());
  }
  
  /**
   * Gets the average delay from when the task is ready, to when 
   * it is actually executed.
   * 
   * @return average delay for low priority tasks to be executed
   */
  public long getLowPriorityAvgExecutionDelay() {
    return getAvgTime(getLowPriorityExecutionDelays());
  }
  
  /**
   * Gets the median delay from when the task is ready, to when 
   * it is actually executed.  Returns -1 if no stats have been 
   * collected yet.
   * 
   * @return median delay for high priority tasks to be executed
   */
  public long getHighPriorityMedianExecutionDelay() {
    List<Long> times;
    synchronized (highPriorityExecutionDelay) {
      times = new ArrayList<Long>(highPriorityExecutionDelay);
    }
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Gets the median delay from when the task is ready, to when 
   * it is actually executed.  Returns -1 if no stats have been 
   * collected yet.
   * 
   * @return median delay for low priority tasks to be executed
   */
  public long getLowPriorityMedianExecutionDelay() {
    List<Long> times;
    synchronized (lowPriorityExecutionDelay) {
      times = new ArrayList<Long>(lowPriorityExecutionDelay);
    }
    if (times.isEmpty()) {
      return -1;
    }
    Collections.sort(times);
    
    return times.get(times.size() / 2);
  }
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  
   * This is the window used for the rolling average for 
   * getHighPriorityAvgExecutionDelay().  This call allows for more complex 
   * statistics (ie looking for outliers, etc).
   * 
   * @return list which represents execution delay samples
   */
  public List<Long> getHighPriorityExecutionDelays() {
    List<Long> result;
    synchronized (highPriorityExecutionDelay) {
      result = new ArrayList<Long>(highPriorityExecutionDelay);
    }
    return Collections.unmodifiableList(result);
  }
  
  /**
   * Call to get a list of all currently recorded times for execution delays.  
   * This is the window used for the rolling average for 
   * getLowPriorityAvgExecutionDelay().  This call allows for more complex 
   * statistics (ie looking for outliers, etc).
   * 
   * @return list which represents execution delay samples
   */
  public List<Long> getLowPriorityExecutionDelays() {
    List<Long> result;
    synchronized (lowPriorityExecutionDelay) {
      result = new ArrayList<Long>(lowPriorityExecutionDelay);
    }
    return Collections.unmodifiableList(result);
  }
  
  private static long getAvgTime(List<Long> list) {
    if (list.isEmpty()) {
      return -1;
    }
    
    long totalTime = 0;
    Iterator<Long> it = list.iterator();
    while (it.hasNext()) {
      totalTime += it.next();
    }
      
    return totalTime / list.size();
  }
  
  /**
   * Call to get a list of all currently recorded times for execution.  
   * This is the window used for the rolling average for 
   * getAverageTaskRunTime().  This call allows for more complex statistics 
   * (ie looking for outliers, etc).
   * 
   * @return the list of currently recorded run times for tasks
   */
  public List<Long> getRunTimes() {
    List<Long> result;
    synchronized (runTimes) {
      result = new ArrayList<Long>(runTimes);
    }
    return Collections.unmodifiableList(result);
  }
  
  /**
   * Call to get the total qty of tasks this executor has handled.
   * 
   * @return total qty of tasks run
   */
  public int getTotalExecutionCount() {
    return getHighPriorityTotalExecutionCount() + 
             getLowPriorityTotalExecutionCount();
  }
  
  /**
   * Call to get the total qty of high priority tasks this executor has handled.
   * 
   * @return total qty of high priority tasks run
   */
  public int getHighPriorityTotalExecutionCount() {
    return totalHighPriorityExecutions.get();
  }
  
  /**
   * Call to get the total qty of low priority tasks this executor has handled.
   * 
   * @return total qty of low priority tasks run
   */
  public int getLowPriorityTotalExecutionCount() {
    return totalLowPriorityExecutions.get();
  }
  
  /**
   * Call to get any {@link Runnable} that have been running longer than a given period of time.  
   * This is particularly useful when looking for runnables that may be executing longer 
   * than expected.  Cases where that happens these runnables could block the thread pool 
   * from executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for
   * @return list of runnables which are, or had been running over the provided time length
   */
  public List<Runnable> getRunnablesRunningOverTime(long timeInMs) {
    List<Runnable> result = new LinkedList<Runnable>();
    
    long now = Clock.accurateTime();
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
   * This is particularly useful when looking for callables that may be executing longer 
   * than expected.  Cases where that happens these callables could block the thread pool 
   * from executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for
   * @return list of callables which are, or had been running over the provided time length
   */
  public List<Callable<?>> getCallablesRunningOverTime(long timeInMs) {
    List<Callable<?>> result = new LinkedList<Callable<?>>();
    
    long now = Clock.accurateTime();
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
   * Call to return the number of callables and/or runnables which have been running longer 
   * than the provided amount of time in milliseconds.
   * 
   * @param timeInMs threshold of time to search for execution
   * @return total qty of runnables and callables which have or are running longer than the provided time length
   */
  public int getQtyRunningOverTime(long timeInMs) {
    int result = 0;
    
    long now = Clock.accurateTime();
    Iterator<Long> it = runningTasks.values().iterator();
    while (it.hasNext()) {
      Long startTime = it.next();
      if (now - startTime >= timeInMs) {
        result++;
      }
    }
    
    return result;
  }
  
  /**
   * Call to check how many tasks are currently being executed 
   * in this thread pool.
   * 
   * @return current number of running tasks
   */
  public int getCurrentlyRunningCount() {
    return runningTasks.size();
  }
  
  /**
   * Call to see how frequently tasks are able to run without creating 
   * a new thread.
   * 
   * Returns -1 if no statistics have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused
   */
  public double getThreadReusePercent() {
    List<Boolean> totalList = new LinkedList<Boolean>();
    synchronized (lowPriorityWorkerAvailable) {
      totalList.addAll(lowPriorityWorkerAvailable);
    }
    synchronized (highPriorityWorkerAvailable) {
      totalList.addAll(highPriorityWorkerAvailable);
    }
    return getTruePercent(totalList);
  }
  
  /**
   * Call to see how frequently high priority tasks are able to run without creating 
   * a new thread.
   * 
   * Returns -1 if no statistics for high priority tasks have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused for high priority tasks
   */
  public double getHighPriorityThreadReusePercent() {
    synchronized (highPriorityWorkerAvailable) {
      return getTruePercent(highPriorityWorkerAvailable);
    }
  }
  
  /**
   * Call to see how frequently low priority tasks are able to run without creating 
   * a new thread.
   * 
   * Returns -1 if no statistics for high priority tasks have been recorded yet.
   * 
   * @return percent of time that threads are able to be reused for low priority tasks
   */
  public double getLowPriorityThreadReusePercent() {
    synchronized (lowPriorityWorkerAvailable) {
      return getTruePercent(lowPriorityWorkerAvailable);
    }
  }
  
  private static double getTruePercent(List<Boolean> list) {
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
  
  protected synchronized void trackTaskStart(Wrapper taskWrapper) {
    runningTasks.put(taskWrapper, Clock.accurateTime());
    
    switch (taskWrapper.priority) {
      case High:
        totalHighPriorityExecutions.incrementAndGet();
        break;
      case Low:
        totalLowPriorityExecutions.incrementAndGet();
        break;
      default:
        throw new UnsupportedOperationException("Priority not handled: " + taskWrapper.priority);
    }
  }
  
  protected void trackTaskFinish(Wrapper taskWrapper) {
    long finishTime = Clock.accurateTime();
    Long startTime = runningTasks.remove(taskWrapper);
    /* start time will never be null if this is called in the same 
     * thread as trackTaskStart and trackTaskStart was called first.  
     * We make that assumption here.
     */
    synchronized (runTimes) {
      runTimes.add(finishTime - startTime);
      trimList(runTimes);
    }
  }
  
  @SuppressWarnings("rawtypes")
  protected static void trimList(LinkedList list) {
    synchronized (list) {
      while (list.size() > MAX_WINDOW_SIZE) {
        list.removeFirst();
      }
    }
  }
  
  /**
   * Wrapper for any task which needs to track statistics.
   * 
   * @author jent - Mike Jensen
   */
  protected class Wrapper {
    public final boolean callable;
    public final TaskPriority priority;
    public final boolean recurring;
    
    public Wrapper(boolean callable, 
                   TaskPriority priority, 
                   boolean recurring) {
      this.callable = callable;
      this.priority = priority;
      this.recurring = recurring;
    }
  }
  
  /**
   * Wrapper for {@link Runnable} for tracking statistics.
   * 
   * @author jent - Mike Jensen
   */
  protected class RunnableStatWrapper extends Wrapper implements Runnable {
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
  }

  /**
   * Wrapper for {@link Callable} for tracking statistics.
   * 
   * @author jent - Mike Jensen
   */
  protected class CallableStatWrapper<T> extends Wrapper implements Callable<T> {
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
  }
}
