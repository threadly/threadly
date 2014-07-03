package org.threadly.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * <p>Executor to run tasks, schedule tasks.  
 * Unlike {@link java.util.concurrent.ScheduledThreadPoolExecutor}
 * this scheduled executor's pool size can grow and shrink based off 
 * usage.  It also has the benefit that you can provide "low priority" 
 * tasks which will attempt to use existing workers and not instantly 
 * create new threads on demand.  Thus allowing you to better take 
 * the benefits of a thread pool for tasks which specific execution 
 * time is less important.</p>
 * 
 * <p>Most tasks provided into this pool will likely want to be 
 * "high priority", to more closely match the behavior of other 
 * thread pools.  That is why unless specified by the constructor, 
 * the default {@link TaskPriority} is High.</p>
 * 
 * <p>When providing a "low priority" task, the task wont execute till 
 * one of the following is true.  The pool is has low load, and there 
 * are available threads already to run on.  The pool has no available 
 * threads, but is under it's max size and has waited the maximum wait 
 * time for a thread to be become available.</p>
 * 
 * <p>In all conditions, "low priority" tasks will never be starved.  
 * They only attempt to allow "high priority" tasks the priority.  
 * This makes "low priority" tasks ideal which do regular cleanup, or 
 * in general anything that must run, but cares little if there is a 
 * 1, or 10 second gap in the execution time.  That amount of tolerance 
 * for "low priority" tasks is adjustable by setting the 
 * maxWaitForLowPriorityInMs either in the constructor, or at runtime.</p>
 * 
 * @deprecated use PriorityScheduler (this class has been renamed) 
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
@Deprecated
public class PriorityScheduledExecutor extends PriorityScheduler {
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
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
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
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
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
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
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
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                   long maxWaitForLowPriorityInMs, 
                                   boolean useDaemonThreads) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, useDaemonThreads);
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
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                   long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    super(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
          defaultPriority, maxWaitForLowPriorityInMs, threadFactory);
  }
}
