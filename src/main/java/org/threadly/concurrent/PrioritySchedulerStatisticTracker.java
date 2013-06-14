package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final int MAX_RUN_TIME_WINDOW_SIZE = 1000;
  
  protected final AtomicInteger totalExecutions;
  protected final ConcurrentHashMap<Wrapper, Long> runningTasks;
  protected final LinkedList<Long> runTimes;
  
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
    
    totalExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
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
    
    totalExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
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
    
    totalExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
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
    
    totalExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
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
    
    totalExecutions = new AtomicInteger();
    runningTasks = new ConcurrentHashMap<Wrapper, Long>();
    runTimes = new LinkedList<Long>();
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
  public void execute(Runnable task, TaskPriority priority) {
    super.execute(wrap(task, priority, false), priority);
  }

  @Override
  public Future<?> submit(Runnable task, TaskPriority priority) {
    return super.submit(wrap(task, priority, false), priority);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task, TaskPriority priority) {
    return super.submit(wrap(task, priority, false), priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    super.schedule(wrap(task, priority, false), 
                   delayInMs, priority);
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs,
                                   TaskPriority priority) {
    return super.submitScheduled(wrap(task, priority, false), 
                                 delayInMs, priority);
  }

  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs,
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
   * Call to get the total qty of tasks this executor has handled.
   * 
   * @return total qty of tasks run
   */
  public int getTotalExecutionCount() {
    return totalExecutions.get();
  }
  
  /**
   * Call to get any runnables that have been running longer than a given period of time.  
   * This is particularly useful when looking for runnables that may be executing longer 
   * than expected.  Cases where that happens these runnables could block the thread pool 
   * from executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for runnables
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
   * Call to get any callables that have been running longer than a given period of time.  
   * This is particularly useful when looking for callables that may be executing longer 
   * than expected.  Cases where that happens these callables could block the thread pool 
   * from executing additional tasks.
   * 
   * @param timeInMs threshold of time to search for callables
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
  
  protected void trackTaskStart(Wrapper taskWrapper) {
    runningTasks.put(taskWrapper, Clock.accurateTime());
    
    totalExecutions.incrementAndGet();
  }
  
  protected void trackTaskFinish(Wrapper taskWrapper) {
    Long startTime = runningTasks.remove(taskWrapper);
    /* start time will never be null if this is called in the same 
     * thread as trackTaskStart and trackTaskStart was called first.  
     * We make that assumption here.
     */
    synchronized (runTimes) {
      runTimes.add(Clock.accurateTime() - startTime);
      while (runTimes.size() > MAX_RUN_TIME_WINDOW_SIZE) {
        runTimes.removeFirst();
      }
    }
  }
  
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
