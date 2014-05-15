package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.threadly.concurrent.BlockingQueueConsumer.ConsumerAcceptor;
import org.threadly.concurrent.collections.DynamicDelayQueue;
import org.threadly.concurrent.collections.DynamicDelayedUpdater;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

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
 * @author jent - Mike Jensen
 * @since 2.0.0 (existed since 1.0.0 as PriorityScheduledExecutor)
 */
public class PriorityScheduler implements PrioritySchedulerInterface {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = true;
  protected static final int WORKER_CONTENTION_LEVEL = 2; // level at which no worker contention is considered
  protected static final int LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS = 2;
  protected static final String QUEUE_CONSUMER_THREAD_NAME_HIGH_PRIORITY;
  protected static final String QUEUE_CONSUMER_THREAD_NAME_LOW_PRIORITY;
  
  static {
    String threadNameSuffix = "task consumer for " + PriorityScheduler.class.getSimpleName();
    QUEUE_CONSUMER_THREAD_NAME_HIGH_PRIORITY = "high priority " + threadNameSuffix;
    QUEUE_CONSUMER_THREAD_NAME_LOW_PRIORITY = "low priority " + threadNameSuffix;
  }
  
  protected final TaskPriority defaultPriority;
  protected final Object highPriorityLock;
  protected final Object lowPriorityLock;
  protected final Object workersLock;
  protected final Object poolSizeChangeLock;
  protected final DynamicDelayQueue<TaskWrapper> highPriorityQueue;
  protected final DynamicDelayQueue<TaskWrapper> lowPriorityQueue;
  protected final Deque<Worker> availableWorkers;        // is locked around workersLock
  protected final ThreadFactory threadFactory;
  protected final TaskConsumer highPriorityConsumer;  // is locked around highPriorityLock
  protected final TaskConsumer lowPriorityConsumer;    // is locked around lowPriorityLock
  protected long lastHighDelay;   // is locked around workersLock
  private final AtomicBoolean shutdownStarted;
  private volatile boolean shutdownFinishing; // once true, never goes to false
  private volatile int corePoolSize;  // can only be changed when poolSizeChangeLock locked
  private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
  private volatile long keepAliveTimeInMs;
  private volatile long maxWaitForLowPriorityInMs;
  private volatile boolean allowCorePoolTimeout;
  private int waitingForWorkerCount;  // is locked around workersLock
  private int currentPoolSize;  // is locked around workersLock

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
  public PriorityScheduler(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs) {
    this(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
         DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         DEFAULT_NEW_THREADS_DAEMON);
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
  public PriorityScheduler(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, boolean useDaemonThreads) {
    this(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
         DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, 
         useDaemonThreads);
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
  public PriorityScheduler(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                   long maxWaitForLowPriorityInMs) {
    this(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
         defaultPriority, maxWaitForLowPriorityInMs, 
         DEFAULT_NEW_THREADS_DAEMON);
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
  public PriorityScheduler(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                   long maxWaitForLowPriorityInMs, 
                                   final boolean useDaemonThreads) {
    
    this(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
         defaultPriority, maxWaitForLowPriorityInMs, 
         new ThreadFactory() {
           private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
          
           @Override
           public Thread newThread(Runnable runnable) {
             Thread thread = defaultFactory.newThread(runnable);
             
             thread.setDaemon(useDaemonThreads);
             
             return thread;
           }
         });
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
  public PriorityScheduler(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs, TaskPriority defaultPriority, 
                                   long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    if (corePoolSize < 1) {
      throw new IllegalArgumentException("corePoolSize must be > 0");
    } else if (maxPoolSize < corePoolSize) {
      throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
    }
    
    //calls to verify and set values
    setKeepAliveTime(keepAliveTimeInMs);
    setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
    
    if (defaultPriority == null) {
      defaultPriority = DEFAULT_PRIORITY;
    }
    if (threadFactory == null) {
      threadFactory = Executors.defaultThreadFactory();
    }
    
    this.defaultPriority = defaultPriority;
    highPriorityLock = new Object();
    lowPriorityLock = new Object();
    workersLock = new Object();
    poolSizeChangeLock = new Object();
    highPriorityQueue = new DynamicDelayQueue<TaskWrapper>(highPriorityLock);
    lowPriorityQueue = new DynamicDelayQueue<TaskWrapper>(lowPriorityLock);
    availableWorkers = new ArrayDeque<Worker>(maxPoolSize);
    this.threadFactory = threadFactory;
    highPriorityConsumer = new TaskConsumer(highPriorityQueue, highPriorityLock, 
                                            new ConsumerAcceptor<TaskWrapper>() {
      @Override
      public void acceptConsumedItem(TaskWrapper task) throws InterruptedException {
        runHighPriorityTask(task);
      }
    });
    lowPriorityConsumer = new TaskConsumer(lowPriorityQueue, lowPriorityLock, 
                                           new ConsumerAcceptor<TaskWrapper>() {
      @Override
      public void acceptConsumedItem(TaskWrapper task) throws InterruptedException {
        runLowPriorityTask(task);
      }
    });
    shutdownStarted = new AtomicBoolean(false);
    shutdownFinishing = false;
    this.corePoolSize = corePoolSize;
    this.maxPoolSize = maxPoolSize;
    this.allowCorePoolTimeout = false;
    this.lastHighDelay = 0;
    waitingForWorkerCount = 0;
    currentPoolSize = 0;
  }
  
  /**
   * If a section of code wants a different default priority, or wanting to provide 
   * a specific default priority in for {@link TaskExecutorDistributor}, 
   * or {@link TaskSchedulerDistributor}.
   * 
   * @param priority default priority for PrioritySchedulerInterface implementation
   * @return a PrioritySchedulerInterface with the default priority specified
   */
  public PrioritySchedulerInterface makeWithDefaultPriority(TaskPriority priority) {
    if (priority == defaultPriority) {
      return this;
    } else {
      return new PrioritySchedulerWrapper(this, priority);
    }
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }
  
  /**
   * Getter for the current set core pool size.
   * 
   * @return current core pool size
   */
  public int getCorePoolSize() {
    return corePoolSize;
  }
  
  /**
   * Getter for the currently set max pool size.
   * 
   * @return current max pool size
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }
  
  /**
   * Getter for the currently set keep alive time.
   * 
   * @return current keep alive time
   */
  public long getKeepAliveTime() {
    return keepAliveTimeInMs;
  }
  
  /**
   * Getter for the current qty of workers constructed (ether running or idle).
   * 
   * @return current worker count
   */
  public int getCurrentPoolSize() {
    synchronized (workersLock) {
      return currentPoolSize;
    }
  }
  
  /**
   * Call to check how many tasks are currently being executed 
   * in this thread pool.
   * 
   * @return current number of running tasks
   */
  public int getCurrentRunningCount() {
    synchronized (workersLock) {
      return currentPoolSize - availableWorkers.size();
    }
  }
  
  /**
   * Change the set core pool size.  If the value is less than the current max 
   * pool size, the max pool size will also be updated to this value.
   * 
   * If this was a reduction from the previous value, this call will examine idle workers 
   * to see if they should be expired.  If this call reduced the max pool size, and the 
   * current running thread count is higher than the new max size, this call will NOT 
   * block till the pool is reduced.  Instead as those workers complete, they will clean 
   * up on their own.
   * 
   * @param corePoolSize New pool size.  Must be at least one.
   */
  public void setCorePoolSize(int corePoolSize) {
    if (corePoolSize < 1) {
      throw new IllegalArgumentException("corePoolSize must be > 0");
    }
    
    synchronized (poolSizeChangeLock) {
      boolean lookForExpiredWorkers = this.corePoolSize > corePoolSize;
      
      if (maxPoolSize < corePoolSize) {
        setMaxPoolSize(corePoolSize);
      }
      
      this.corePoolSize = corePoolSize;
      
      if (lookForExpiredWorkers) {
        expireOldWorkers();
      }
    }
  }
  
  /**
   * Change the set max pool size.  If the value is less than the current core 
   * pool size, the core pool size will be reduced to match the new max pool size.  
   * 
   * If this was a reduction from the previous value, this call will examine idle workers 
   * to see if they should be expired.  If the current running thread count is higher 
   * than the new max size, this call will NOT block till the pool is reduced.  
   * Instead as those workers complete, they will clean up on their own.
   * 
   * @param maxPoolSize New max pool size.  Must be at least one.
   */
  public void setMaxPoolSize(int maxPoolSize) {
    if (maxPoolSize < 1) {
      throw new IllegalArgumentException("maxPoolSize must be > 0");
    }
    
    synchronized (poolSizeChangeLock) {
      boolean poolSizeIncrease = maxPoolSize < this.maxPoolSize;
      
      if (maxPoolSize < corePoolSize) {
        this.corePoolSize = maxPoolSize;
      }
      
      this.maxPoolSize = maxPoolSize;
      
      if (poolSizeIncrease) {
        // now that pool size increased, start any workers we can for the waiting tasks
        synchronized (workersLock) {
          if (waitingForWorkerCount > 0) {
            while (availableWorkers.size() < waitingForWorkerCount && 
                   currentPoolSize < this.maxPoolSize) {
              availableWorkers.add(makeNewWorker());
            }
            
            workersLock.notifyAll();
          }
        }
      } else {
        expireOldWorkers();
      }
    }
  }
  
  /**
   * Change the set idle thread keep alive time.  If this is a reduction in the 
   * previously set keep alive time, this call will then check for expired worker 
   * threads.
   * 
   * @param keepAliveTimeInMs New keep alive time in milliseconds.  Must be at least zero.
   */
  public void setKeepAliveTime(long keepAliveTimeInMs) {
    if (keepAliveTimeInMs < 0) {
      throw new IllegalArgumentException("keepAliveTimeInMs must be >= 0");
    }
    
    boolean checkForExpiredWorkers = this.keepAliveTimeInMs > keepAliveTimeInMs;
    
    this.keepAliveTimeInMs = keepAliveTimeInMs;
    
    if (checkForExpiredWorkers) {
      expireOldWorkers();
    }
  }
  
  /**
   * Changes the max wait time for an idle worker for low priority tasks.
   * Changing this will only take effect for future low priority tasks, it 
   * will have no impact for the current low priority task attempting to get 
   * a worker.
   * 
   * @param maxWaitForLowPriorityInMs new time to wait for a thread in milliseconds.  Must be at least zero.
   */
  public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
    if (maxWaitForLowPriorityInMs < 0) {
      throw new IllegalArgumentException("maxWaitForLowPriorityInMs must be >= 0");
    }
    
    this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
  }
  
  /**
   * Getter for the maximum amount of time a low priority task will 
   * wait for an available worker.
   * 
   * @return currently set max wait for low priority task
   */
  public long getMaxWaitForLowPriority() {
    return maxWaitForLowPriorityInMs;
  }
  
  /**
   * Returns how many tasks are either waiting to be executed, 
   * or are scheduled to be executed at a future point.
   * 
   * @return qty of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount() {
    return highPriorityQueue.size() + lowPriorityQueue.size();
  }
  
  /**
   * Returns a count of how many tasks are either waiting to be executed, 
   * or are scheduled to be executed at a future point for a specific priority.
   * 
   * @param priority priority for tasks to be counted
   * @return qty of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getScheduledTaskCount();
    }
    
    switch (priority) {
      case High:
        return highPriorityQueue.size();
      case Low:
        return lowPriorityQueue.size();
      default:
        throw new UnsupportedOperationException("Not implemented for priority: " + priority);
    }
  }
  
  /**
   * Prestarts all core threads.  This will make new idle workers to accept future tasks.
   */
  public void prestartAllCoreThreads() {
    synchronized (workersLock) {
      boolean startedThreads = false;
      while (currentPoolSize < corePoolSize) {
        availableWorkers.addFirst(makeNewWorker());
        startedThreads = true;
      }
      
      if (startedThreads) {
        workersLock.notifyAll();
      }
    }
  }

  /**
   * Changes the setting weather core threads are allowed to be killed 
   * if they remain idle.  If changing to allow core thread timeout, 
   * this call will then perform a check to look for expired workers.
   * 
   * @param value true if core threads should be expired when idle.
   */
  public void allowCoreThreadTimeOut(boolean value) {
    boolean checkForExpiredWorkers = ! allowCorePoolTimeout && value;
    
    allowCorePoolTimeout = value;
    
    if (checkForExpiredWorkers) {
      expireOldWorkers();
    }
  }

  @Override
  public boolean isShutdown() {
    return shutdownStarted.get();
  }
  
  protected List<Runnable> clearTaskQueue() {
    synchronized (highPriorityLock) {
      synchronized (lowPriorityLock) {
        highPriorityConsumer.stop();
        lowPriorityConsumer.stop();
        List<Runnable> removedTasks = new ArrayList<Runnable>(highPriorityQueue.size() + 
                                                                lowPriorityQueue.size());
        
        synchronized (highPriorityQueue.getLock()) {
          Iterator<TaskWrapper> it = highPriorityQueue.iterator();
          while (it.hasNext()) {
            TaskWrapper tw = it.next();
            tw.cancel();
            removedTasks.add(tw.task);
          }
          lowPriorityQueue.clear();
        }
        synchronized (lowPriorityQueue.getLock()) {
          Iterator<TaskWrapper> it = lowPriorityQueue.iterator();
          while (it.hasNext()) {
            TaskWrapper tw = it.next();
            tw.cancel();
            removedTasks.add(tw.task);
          }
          lowPriorityQueue.clear();
        }
        
        return removedTasks;
      }
    }
  }
  
  protected void shutdownAllWorkers() {
    synchronized (workersLock) {
      Iterator<Worker> it = availableWorkers.iterator();
      while (it.hasNext()) {
        Worker w = it.next();
        it.remove();
        killWorker(w);
      }
      
      // we notify all in case some are waiting for shutdown
      workersLock.notifyAll();
    }
  }

  /**
   * Stops any new tasks from being submitted to the pool.  But allows all currently scheduled 
   * tasks to be run.  If scheduled tasks are present they will also be unable to reschedule.
   * 
   * If you wish to not want to run any queued tasks you should use {#link shutdownNow()).
   */
  public void shutdown() {
    if (! shutdownStarted.getAndSet(true)) {
      addToHighPriorityQueue(new OneTimeTaskWrapper(new ShutdownRunnable(), 
                                                    TaskPriority.High, 1));
    }
  }

  /**
   * Stops any new tasks from being able to be executed and removes workers from the pool.
   * 
   * This implementation refuses new submissions after this call.  And will NOT interrupt any 
   * tasks which are currently running.  But any tasks which are waiting in queue to be run 
   * (but have not started yet), will not be run.  Those waiting tasks will be removed, and 
   * as workers finish with their current tasks the threads will be joined.
   * 
   * @return List of runnables which were waiting to execute
   */
  public List<Runnable> shutdownNow() {
    shutdownStarted.set(true);
    shutdownFinishing = true;
    List<Runnable> awaitingTasks = clearTaskQueue();
    shutdownAllWorkers();
    
    return awaitingTasks;
  }
  
  /**
   * Check weather the shutdown process is finished.
   * 
   * @return true if the scheduler is finishing its shutdown
   */
  protected boolean getShutdownFinishing() {
    return shutdownFinishing;
  }
  
  /**
   * Makes a new {@link PrioritySchedulerLimiter} that uses this pool as it's execution source.
   * 
   * @param maxConcurrency maximum number of threads to run in parallel in sub pool
   * @return newly created {@link PrioritySchedulerLimiter} that uses this pool as it's execution source
   */
  public PrioritySchedulerInterface makeSubPool(int maxConcurrency) {
    return makeSubPool(maxConcurrency, null);
  }

  /**  
   * Makes a new {@link PrioritySchedulerLimiter} that uses this pool as it's execution source.
   * 
   * @param maxConcurrency maximum number of threads to run in parallel in sub pool
   * @param subPoolName name to describe threads while running under this sub pool
   * @return newly created {@link PrioritySchedulerLimiter} that uses this pool as it's execution source
   */
  public PrioritySchedulerInterface makeSubPool(int maxConcurrency, String subPoolName) {
    if (maxConcurrency > maxPoolSize) {
      throw new IllegalArgumentException("A sub pool should be smaller than the parent pool");
    }
    
    return new PrioritySchedulerLimiter(this, maxConcurrency, subPoolName);
  }
  
  protected static boolean removeFromTaskQueue(DynamicDelayQueue<TaskWrapper> queue, 
                                               Runnable task) {
    synchronized (queue.getLock()) {
      Iterator<TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        if (ContainerHelper.isContained(tw.task, task)) {
          tw.cancel();
          it.remove();
          
          return true;
        }
      }
    }
    
    return false;
  }
  
  protected static boolean removeFromTaskQueue(DynamicDelayQueue<TaskWrapper> queue, 
                                               Callable<?> task) {
    synchronized (queue.getLock()) {
      Iterator<TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        if (ContainerHelper.isContained(tw.task, task)) {
          tw.cancel();
          it.remove();
          
          return true;
        }
      }
    }
    
    return false;
  }

  /**
   * Removes the runnable task from the execution queue.  It is possible for the 
   * runnable to still run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task 
   * (as in a complete guarantee).  But while this task is called, it will 
   * reduce the throughput of execution, so should not be used extremely 
   * frequently.
   * 
   * @param task The original task provided to the executor
   * @return true if the task was found and removed
   */
  @Override
  public boolean remove(Runnable task) {
    return removeFromTaskQueue(highPriorityQueue, task) || 
             removeFromTaskQueue(lowPriorityQueue, task);
  }

  /**
   * Removes the callable task from the execution queue.  It is possible for the 
   * callable to still run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task 
   * (as in a complete guarantee).  But while this task is called, it will 
   * reduce the throughput of execution, so should not be used extremely 
   * frequently.
   * 
   * @param task The original callable provided to the executor
   * @return true if the callable was found and removed
   */
  @Override
  public boolean remove(Callable<?> task) {
    return removeFromTaskQueue(highPriorityQueue, task) || 
             removeFromTaskQueue(lowPriorityQueue, task);
  }

  @Override
  public void execute(Runnable task) {
    schedule(task, 0, defaultPriority);
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
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
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submitScheduled(task, null, 0, priority);
  }
  
  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, TaskPriority priority) {
    return submitScheduled(task, result, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return submitScheduled(task, 0, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = defaultPriority;
    }

    addToQueue(new OneTimeTaskWrapper(task, priority, delayInMs));
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return submitScheduled(task, result, delayInMs, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, 
                                                 TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = defaultPriority;
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task, result);
    addToQueue(new OneTimeTaskWrapper(rf, priority, delayInMs));
    
    return rf;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = defaultPriority;
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task);
    addToQueue(new OneTimeTaskWrapper(rf, priority, delayInMs));
    
    return rf;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                           defaultPriority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    if (priority == null) {
      priority = defaultPriority;
    }

    addToQueue(new RecurringTaskWrapper(task, priority, initialDelay, recurringDelay));
  }
  
  protected void addToQueue(TaskWrapper task) {
    if (shutdownStarted.get()) {
      throw new IllegalStateException("Thread pool shutdown");
    }
    
    switch (task.priority) {
      case High:
        addToHighPriorityQueue(task);
        break;
      case Low:
        addToLowPriorityQueue(task);
        break;
      default:
        throw new UnsupportedOperationException("Priority not implemented: " + task.priority);
    }
  }
  
  private void addToHighPriorityQueue(TaskWrapper task) {
    ClockWrapper.stopForcingUpdate();
    try {
      highPriorityQueue.add(task);
    } finally {
      ClockWrapper.resumeForcingUpdate();
    }
    highPriorityConsumer.maybeStart(threadFactory, 
                                    QUEUE_CONSUMER_THREAD_NAME_HIGH_PRIORITY);
  }
  
  private void addToLowPriorityQueue(TaskWrapper task) {
    ClockWrapper.stopForcingUpdate();
    try {
      lowPriorityQueue.add(task);
    } finally {
      ClockWrapper.resumeForcingUpdate();
    }
    lowPriorityConsumer.maybeStart(threadFactory, 
                                   QUEUE_CONSUMER_THREAD_NAME_LOW_PRIORITY);
  }
  
  
  /**
   * This function REQUIRES that workersLock is synchronized before calling.
   * 
   * @param maxWaitTimeInMs time to wait for a worker to become available
   * @return an available worker, or null if no worker became available within the maxWaitTimeInMs
   * @throws InterruptedException Thrown if thread is interrupted while waiting for worker
   */
  protected Worker getExistingWorker(long maxWaitTimeInMs) throws InterruptedException {
    long startTime = Clock.accurateTime();
    waitingForWorkerCount++;
    try {
      long waitTime = maxWaitTimeInMs;
      while (availableWorkers.isEmpty() && waitTime > 0) {
        if (waitTime == Long.MAX_VALUE) {  // prevent overflow
          workersLock.wait();
        } else {
          long elapsedTime = Clock.accurateTime() - startTime;
          waitTime = maxWaitTimeInMs - elapsedTime;
          if (waitTime > 0) {
            workersLock.wait(waitTime);
          }
        }
      }
      
      if (availableWorkers.isEmpty()) {
        return null;  // we exceeded the wait time
      } else {
        // always remove from the front, to get the newest worker
        return availableWorkers.removeFirst();
      }
    } finally {
      waitingForWorkerCount--;
    }
  }
  
  /**
   * This function REQUIRES that workersLock is synchronized before calling.
   * 
   * @return Newly created worker, started and ready to accept work
   */
  protected Worker makeNewWorker() {
    Worker w = new Worker();
    currentPoolSize++;
    w.start();
    
    // will be added to available workers when done with first task
    return w;
  }
  
  protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (! shutdownFinishing) {
        if (currentPoolSize >= maxPoolSize) {
          lastHighDelay = task.getDelayEstimateInMillis();
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
      w.nextTask(task);
    }
  }
  
  protected void runLowPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (! shutdownFinishing) {
        // wait for high priority tasks that have been waiting longer than us if all workers are consumed
        long waitAmount;
        while (currentPoolSize >= maxPoolSize && 
               availableWorkers.size() < WORKER_CONTENTION_LEVEL &&   // only care if there is worker contention
               ! shutdownFinishing &&
               ! highPriorityQueue.isEmpty() && // if there are no waiting high priority tasks, we don't care 
               (waitAmount = task.getDelayEstimateInMillis() - lastHighDelay) > LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS) {
          workersLock.wait(waitAmount);
          Clock.accurateTime(); // update for getDelayEstimateInMillis
        }
        // check if we should reset the high delay for future low priority tasks
        if (highPriorityQueue.isEmpty()) {
          lastHighDelay = 0;
        }
        
        if (! shutdownFinishing) {  // check again that we are still running
          long waitTime;
          if (currentPoolSize >= maxPoolSize) {
            waitTime = Long.MAX_VALUE;
          } else {
            waitTime = maxWaitForLowPriorityInMs;
          }
          w = getExistingWorker(waitTime);
          if (w == null) {
            // this means we expired past our wait time, so create a worker if we can
            if (currentPoolSize >= maxPoolSize) {
              // more workers were created while waiting, now have reached our max
              w = getExistingWorker(Long.MAX_VALUE);
            } else {
              w = makeNewWorker();
            }
          }
        }
      }
    }
    
    if (w != null) {  // may be null if shutdown
      w.nextTask(task);
    }
  }
  
  protected void expireOldWorkers() {
    synchronized (workersLock) {
      long now = Clock.lastKnownTimeMillis();
      // we search backwards because the oldest workers will be at the back of the stack
      while ((currentPoolSize > corePoolSize || allowCorePoolTimeout) && 
             ! availableWorkers.isEmpty() && 
             (now - availableWorkers.getLast().getLastRunTime() > keepAliveTimeInMs || 
                currentPoolSize > maxPoolSize)) {  // it does not matter how old it is, the max pool size has changed
        Worker w = availableWorkers.removeLast();
        killWorker(w);
      }
    }
  }
  
  private void killWorker(Worker w) {
    synchronized (workersLock) {
      /* we check running around workersLock since we want to make sure 
       * we don't decrement the pool size more than once for a single worker
       */
      if (w.running) {
        currentPoolSize--;
        // it may not always be here, but it sometimes can (for example when a worker is interrupted)
        availableWorkers.remove(w);
        w.stop(); // will set running to false for worker
      }
    }
  }
  
  protected void workerDone(Worker worker) {
    synchronized (workersLock) {
      if (shutdownFinishing) {
        killWorker(worker);
      } else {
        // always add to the front so older workers are at the back
        availableWorkers.addFirst(worker);
      
        expireOldWorkers();
            
        workersLock.notify();
      }
    }
  }
  
  /**
   * <p>Runnable which will consume tasks from the appropriate 
   * and given the provided implementation to get a worker 
   * and execute consumed tasks.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class TaskConsumer extends BlockingQueueConsumer<TaskWrapper> {
    private final Object queueLock;
    
    public TaskConsumer(DynamicDelayQueue<TaskWrapper> queue,
                        Object queueLock, 
                        ConsumerAcceptor<TaskWrapper> taskAcceptor) {
      super(queue, taskAcceptor);
      
      this.queueLock = queueLock;
    }

    @Override
    public TaskWrapper getNext() throws InterruptedException {
      TaskWrapper task;
      /* must lock as same lock for removal to 
       * ensure that task can be found for removal
       */
      synchronized (queueLock) {
        task = queue.take();
        task.executing();  // for recurring tasks this will put them back into the queue
      }
      
      return task;
    }
  }
  
  /**
   * <p>Runnable which will run on pool threads.  It 
   * accepts runnables to run, and tracks usage.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class Worker implements Runnable {
    protected final Thread thread;
    private volatile long lastRunTime;
    private volatile boolean running;
    private volatile TaskWrapper nextTask;
    
    protected Worker() {
      thread = threadFactory.newThread(this);
      running = false;
      lastRunTime = Clock.lastKnownTimeMillis();
      nextTask = null;
    }
    
    // should only be called from killWorker
    public void stop() {
      if (! running) {
        return;
      } else {
        running = false;
        
        LockSupport.unpark(thread);
      }
    }

    public void start() {
      if (running) {
        return;
      } else {
        running = true;
        thread.start();
      }
    }
    
    public void nextTask(TaskWrapper task) {
      if (! running) {
        throw new IllegalStateException();
      } else if (nextTask != null) {
        throw new IllegalStateException();
      }
      
      nextTask = task;

      LockSupport.unpark(thread);
    }
    
    public void blockTillNextTask() {
      boolean checkedInterrupted = false;
      while (nextTask == null && running) {
        LockSupport.park(this);

        checkInterrupted();
        checkedInterrupted = true;
      }
      
      if (! checkedInterrupted) {
        // must verify thread is not in interrupted status before it runs a task
        checkInterrupted();
      }
    }
    
    private void checkInterrupted() {
      if (Thread.interrupted()) { // check and clear interrupt
        if (shutdownFinishing) {
          /* If provided a new task, by the time killWorker returns we will still run that task 
           * before letting the thread return.
           */
          killWorker(this);
        }
      }
    }
    
    @Override
    public void run() {
      while (running) {
        try {
          blockTillNextTask();
          
          if (nextTask != null) {
            nextTask.run();
          }
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        } finally {
          nextTask = null;
          if (running) {
            // only check if still running, otherwise worker has already been killed
            lastRunTime = Clock.lastKnownTimeMillis();
            workerDone(this);
          }
        }
      }
    }
    
    public long getLastRunTime() {
      return lastRunTime;
    }
  }
  
  /**
   * <p>Abstract implementation for all tasks handled by this pool.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract static class TaskWrapper extends AbstractDelayed 
                                              implements Runnable {
    public final TaskPriority priority;
    protected final Runnable task;
    protected volatile boolean canceled;
    
    public TaskWrapper(Runnable task, 
                       TaskPriority priority) {
      this.priority = priority;
      this.task = task;
      canceled = false;
    }
    
    public void cancel() {
      canceled = true;
      
      if (task instanceof Future<?>) {
        ((Future<?>)task).cancel(false);
      }
    }
    
    public abstract void executing();
    
    protected abstract long getDelayEstimateInMillis();
    
    @Override
    public String toString() {
      return task.toString();
    }
  }
  
  /**
   * <p>Wrapper for tasks which only executes once.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class OneTimeTaskWrapper extends TaskWrapper {
    private final long runTime;
    
    protected OneTimeTaskWrapper(Runnable task, TaskPriority priority, long delay) {
      super(task, priority);
      
      runTime = Clock.accurateTime() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(runTime - ClockWrapper.getSemiAccurateTime(), 
                          TimeUnit.MILLISECONDS);
    }
    
    @Override
    protected long getDelayEstimateInMillis() {
      return runTime - Clock.lastKnownTimeMillis();
    }
    
    @Override
    public void executing() {
      // ignored
    }

    @Override
    public void run() {
      if (! canceled) {
        task.run();
      }
    }
  }
  
  /**
   * <p>Wrapper for tasks which reschedule after completion.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class RecurringTaskWrapper extends TaskWrapper 
                                       implements DynamicDelayedUpdater {
    private final long recurringDelay;
    //private volatile long maxExpectedRuntime;
    private volatile boolean executing;
    private long nextRunTime;
    
    protected RecurringTaskWrapper(Runnable task, TaskPriority priority, 
                                   long initialDelay, long recurringDelay) {
      super(task, priority);
      
      this.recurringDelay = recurringDelay;
      //maxExpectedRuntime = -1;
      executing = false;
      this.nextRunTime = Clock.accurateTime() + initialDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      if (executing) {
        return Long.MAX_VALUE;
      } else {
        return unit.convert(getNextDelayInMillis(), 
                            TimeUnit.MILLISECONDS);
      }
    }
    
    private long getNextDelayInMillis() {
      return nextRunTime - ClockWrapper.getSemiAccurateTime();
    }
    
    @Override
    protected long getDelayEstimateInMillis() {
      return nextRunTime - Clock.lastKnownTimeMillis();
    }

    @Override
    public void allowDelayUpdate() {
      executing = false;
    }
    
    @Override
    public void executing() {
      if (canceled) {
        return;
      }
      executing = true;
      /* add to queue before started, so that it can be removed if necessary
       * We add to the end because the task wont re-run till it has finished, 
       * so there is no reason to sort at this point
       */
      switch (priority) {
        case High:
          highPriorityQueue.addLast(this);
          break;
        case Low:
          lowPriorityQueue.addLast(this);
          break;
        default:
          throw new UnsupportedOperationException("Not implemented for priority: " + priority);
      }
    }
    
    private void reschedule() {
      nextRunTime = Clock.accurateTime() + recurringDelay;
      
      // now that nextRunTime has been set, resort the queue
      switch (priority) {
        case High:
          synchronized (highPriorityLock) {
            if (! shutdownStarted.get()) {
              ClockWrapper.stopForcingUpdate();
              try {
                highPriorityQueue.reposition(this, getNextDelayInMillis(), this);
              } finally {
                ClockWrapper.resumeForcingUpdate();
              }
            }
          }
          break;
        case Low:
          synchronized (lowPriorityLock) {
            if (! shutdownStarted.get()) {
              ClockWrapper.stopForcingUpdate();
              try {
                lowPriorityQueue.reposition(this, getNextDelayInMillis(), this);
              } finally {
                ClockWrapper.resumeForcingUpdate();
              }
            }
          }
          break;
        default:
          throw new UnsupportedOperationException("Not implemented for priority: " + priority);
      }
    }

    @Override
    public void run() {
      if (canceled) {
        return;
      }
      try {
        //long startTime = ClockWrapper.getLastKnownTime();
        
        task.run();
        
        /*long runTime = ClockWrapper.getLastKnownTime() - startTime;
        if (runTime > maxExpectedRuntime) {
          maxExpectedRuntime = runTime;
        }*/
      } finally {
        if (! canceled) {
          try {
            reschedule();
          } catch (java.util.NoSuchElementException e) {
            if (canceled) {
              /* this is a possible condition where shutting down 
               * the thread pool occurred while rescheduling the item. 
               * 
               * Since this is unlikely, we just swallow the exception here.
               */
            } else {
              /* This condition however would not be expected, 
               * so we should throw the exception.
               */
              throw e;
            }
          }
        }
      }
    }
  }
  
  /**
   * <p>Runnable to be run after all current tasks to finish the shutdown sequence.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class ShutdownRunnable implements Runnable {
    @Override
    public void run() {
      shutdownNow();
    }
  }
}
