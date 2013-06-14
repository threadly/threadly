package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

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
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, defaultPriority, maxWaitForLowPriorityInMs, threadFactory);
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
  
  protected class Wrapper {
    public final TaskPriority priority;
    public final boolean recurring;
    
    public Wrapper(TaskPriority priority, boolean recurring) {
      this.priority = priority;
      this.recurring = recurring;
    }
  }
  
  protected class RunnableStatWrapper extends Wrapper implements Runnable {
    private final Runnable toRun;
    
    public RunnableStatWrapper(Runnable toRun, 
                               TaskPriority priority, 
                               boolean recurring) {
      super(priority, recurring);
      
      this.toRun = toRun;
    }
    
    @Override
    public void run() {
      // TODO - track start
      try {
        toRun.run();
      } finally {
        // TODO - track finish
      }
    }
  }
  
  protected class CallableStatWrapper<T> extends Wrapper implements Callable<T> {
    private final Callable<T> toRun;
    
    public CallableStatWrapper(Callable<T> toRun, 
                               TaskPriority priority, 
                               boolean recurring) {
      super(priority, recurring);
      
      this.toRun = toRun;
    }
    
    @Override
    public T call() throws Exception {
      // TODO - track start
      try {
        return toRun.call();
      } finally {
        // TODO - track finish
      }
    }
  }
}
