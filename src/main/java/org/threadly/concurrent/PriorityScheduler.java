package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.ListUtils;

/**
 * <p>Executor to run tasks, schedule tasks.  Unlike 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} this scheduled executor's pool size 
 * can shrink if set with a lower value via {@link #setPoolSize(int)}.  It also has the benefit 
 * that you can provide "low priority" tasks which will attempt to use existing workers and not 
 * instantly create new threads on demand.  Thus allowing you to better take the benefits of a 
 * thread pool for tasks which specific execution time is less important.</p>
 * 
 * <p>Most tasks provided into this pool will likely want to be "high priority", to more closely 
 * match the behavior of other thread pools.  That is why unless specified by the constructor, the 
 * default {@link TaskPriority} is High.</p>
 * 
 * <p>When providing a "low priority" task, the task wont execute till one of the following is 
 * true.  The pool is has low load, and there are available threads already to run on.  The pool 
 * has no available threads, but is under it's max size and has waited the maximum wait time for a 
 * thread to be become available.</p>
 * 
 * <p>In all conditions, "low priority" tasks will never be starved.  They only attempt to allow 
 * "high priority" tasks the priority.  This makes "low priority" tasks ideal which do regular 
 * cleanup, or in general anything that must run, but cares little if there is a 1, or 10 second 
 * gap in the execution time.  That amount of tolerance for "low priority" tasks is adjustable by 
 * setting the {@code maxWaitForLowPriorityInMs} either in the constructor, or at runtime.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0 (existed since 1.0.0 as PriorityScheduledExecutor)
 */
public class PriorityScheduler extends AbstractSubmitterScheduler 
                               implements PrioritySchedulerInterface {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = true;
  protected static final int WORKER_CONTENTION_LEVEL = 2; // level at which no worker contention is considered
  protected static final int LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS = 2;
  protected static final String QUEUE_CONSUMER_THREAD_NAME_SUFFIX;
  // tuned for performance of scheduled tasks
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  static {
    QUEUE_CONSUMER_THREAD_NAME_SUFFIX = " priority task consumer for " + PriorityScheduler.class.getSimpleName();
  }
  
  protected final WorkerPool workerPool;
  protected final TaskPriority defaultPriority;
  protected final QueueManager highPriorityConsumer;  // is locked around highPriorityLock
  protected final QueueManager lowPriorityConsumer;    // is locked around lowPriorityLock

  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.  It also  defaults to all newly created 
   * threads being daemon threads.
   * 
   * @param poolSize Thread pool size that should be maintained
   */
  public PriorityScheduler(int poolSize) {
    this(poolSize, null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, DEFAULT_NEW_THREADS_DAEMON);
  }
  
  /**
   * Constructs a new thread pool, though no threads will be started till it accepts it's first 
   * request.  This constructs a default priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PriorityScheduler(int poolSize, boolean useDaemonThreads) {
    this(poolSize, null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, useDaemonThreads);
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
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
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
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs, 
                           boolean useDaemonThreads) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         new ConfigurableThreadFactory(PriorityScheduler.class.getSimpleName() + "-", 
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
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    this(new WorkerPool(threadFactory, poolSize, maxWaitForLowPriorityInMs), 
         defaultPriority);
  }
  
  /**
   * This constructor is designed for extending classes to be able to provide their own 
   * implementation of {@link WorkerPool}.  Ultimately all constructors will defer to this one.
   * 
   * @param workerPool WorkerPool to handle accepting tasks and providing them to a worker for execution
   * @param defaultPriority Default priority to store in case no priority is provided for tasks
   */
  protected PriorityScheduler(WorkerPool workerPool, TaskPriority defaultPriority) {
    if (defaultPriority == null) {
      defaultPriority = DEFAULT_PRIORITY;
    }
    
    this.workerPool = workerPool;
    this.defaultPriority = defaultPriority;
    highPriorityConsumer = new QueueManager(workerPool, TaskPriority.High, 
                                            TaskPriority.High + QUEUE_CONSUMER_THREAD_NAME_SUFFIX);
    lowPriorityConsumer = new QueueManager(workerPool, TaskPriority.Low, 
                                           TaskPriority.Low + QUEUE_CONSUMER_THREAD_NAME_SUFFIX);
  }
  
  /**
   * If a section of code wants a different default priority, or wanting to provide a specific 
   * default priority in for {@link KeyDistributedExecutor}, or {@link KeyDistributedScheduler}.
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
   * Getter for the currently set max thread pool size.
   * 
   * @return current max pool size
   */
  public int getMaxPoolSize() {
    return workerPool.getMaxPoolSize();
  }
  
  /**
   * Getter for the current quantity of workers/threads constructed (either running or idle).  
   * This is different than the size returned from {@link #getMaxPoolSize()} in that we 
   * lazily create threads.  This represents the amount of threads needed to be created so far, 
   * where {@link #getMaxPoolSize()} represents the amount of threads the pool may grow to.
   * 
   * @return current worker count
   */
  public int getCurrentPoolSize() {
    return workerPool.getCurrentPoolSize();
  }
  
  /**
   * Call to check how many tasks are currently being executed in this thread pool.  Unlike 
   * {@link #getCurrentPoolSize()}, this count will NOT include idle threads waiting to execute 
   * tasks.
   * 
   * @return current number of running tasks
   */
  public int getCurrentRunningCount() {
    return workerPool.getCurrentRunningCount();
  }
  
  /**
   * Change the set thread pool size.
   * 
   * If the value is less than the current running threads, as threads finish they will exit 
   * rather than accept new tasks.  No currently running tasks will be interrupted, rather we 
   * will just wait for them to finish before killing the thread.
   * 
   * If this is an increase in the pool size, threads will be lazily started as needed till the 
   * new size is reached.  If there are tasks waiting for threads to run on, they immediately 
   * will be started.
   * 
   * @param newPoolSize New core pool size, must be at least one
   */
  public void setPoolSize(int newPoolSize) {
    workerPool.setPoolSize(newPoolSize);
  }
  
  /**
   * Changes the max wait time for an idle worker for low priority tasks.  Changing this will only 
   * take effect for future low priority tasks, it will have no impact for the current low priority 
   * task attempting to get a worker.
   * 
   * @param maxWaitForLowPriorityInMs new time to wait for a thread in milliseconds
   */
  public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
    workerPool.setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
  }
  
  /**
   * Getter for the maximum amount of time a low priority task will wait for an available worker.
   * 
   * @return currently set max wait for low priority task
   */
  public long getMaxWaitForLowPriority() {
    return workerPool.getMaxWaitForLowPriority();
  }
  
  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount() {
    return highPriorityConsumer.queueSize() + lowPriorityConsumer.queueSize();
  }
  
  /**
   * Returns a count of how many tasks are either waiting to be executed, or are scheduled to be 
   * executed at a future point for a specific priority.
   * 
   * @param priority priority for tasks to be counted
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getScheduledTaskCount();
    }
    
    return getQueueManager(priority).queueSize();
  }
  
  /**
   * Ensures all threads have been started, it will create threads till the thread count matches 
   * the set pool size (checked via {@link #getMaxPoolSize()}).  If this is able to start threads 
   * (meaning that many threads are not already running), those threads will remain idle till 
   * there is tasks ready to execute.
   */
  public void prestartAllThreads() {
    workerPool.prestartAllThreads();
  }

  @Override
  public boolean isShutdown() {
    return workerPool.isShutdownStarted();
  }

  /**
   * Stops any new tasks from being submitted to the pool.  But allows all tasks which are 
   * submitted to execute, or scheduled (and have elapsed their delay time) to run.  If recurring 
   * tasks are present they will also be unable to reschedule.  If {@code shutdown()} or 
   * {@link #shutdownNow()} has already been called, this will have no effect.  
   * 
   * If you wish to not want to run any queued tasks you should use {@link #shutdownNow()}.
   */
  public void shutdown() {
    if (workerPool.startShutdown()) {
      ShutdownRunnable sr = new ShutdownRunnable(workerPool, lowPriorityConsumer, highPriorityConsumer);
      highPriorityConsumer.addExecute(new OneTimeTaskWrapper(sr,  1));
    }
  }
  
  /**
   * Stops task consumers, and clears all waiting tasks (low and high priority).  It is expected 
   * that {@code shutdownStarted} has transitioned to {@code true} before calling this.  If 
   * {@code shutdownFinishing} has transitioned then unexecuted tasks may fail to be returned.
   * 
   * @return A list of Runnables that had been removed from the queues
   */
  protected List<Runnable> clearTaskQueue() {
    List<Runnable> removedTasks = new ArrayList<Runnable>(getScheduledTaskCount());
    lowPriorityConsumer.stopAndDrainQueueInto(removedTasks);
    highPriorityConsumer.stopAndDrainQueueInto(removedTasks);
    
    return removedTasks;
  }

  /**
   * Stops any new tasks from being able to be executed and removes workers from the pool.
   * 
   * This implementation refuses new submissions after this call.  And will NOT interrupt any 
   * tasks which are currently running.  However any tasks which are waiting in queue to be run 
   * (but have not started yet), will not be run.  Those waiting tasks will be removed, and as 
   * workers finish with their current tasks the threads will be joined.
   * 
   * @return List of runnables which were waiting to execute
   */
  public List<Runnable> shutdownNow() {
    workerPool.startShutdown();
    List<Runnable> awaitingTasks = clearTaskQueue();
    workerPool.finishShutdown();
    
    return awaitingTasks;
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
    if (maxConcurrency > workerPool.getMaxPoolSize()) {
      throw new IllegalArgumentException("A sub pool should be smaller than the parent pool");
    }
    
    return new PrioritySchedulerLimiter(this, maxConcurrency, subPoolName);
  }

  /**
   * Removes the runnable task from the execution queue.  It is possible for the runnable to still 
   * run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task (as in a complete 
   * guarantee).  But while this task is called, it will reduce the throughput of execution, so 
   * should not be used extremely frequently.
   * 
   * @param task The original task provided to the executor
   * @return {@code true} if the task was found and removed
   */
  @Override
  public boolean remove(Runnable task) {
    return highPriorityConsumer.remove(task) || lowPriorityConsumer.remove(task);
  }

  /**
   * Removes the callable task from the execution queue.  It is possible for the callable to still 
   * run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task (as in a complete 
   * guarantee).  But while this task is called, it will reduce the throughput of execution, so 
   * should not be used extremely frequently.
   * 
   * @param task The original callable provided to the executor
   * @return {@code true} if the callable was found and removed
   */
  @Override
  public boolean remove(Callable<?> task) {
    return highPriorityConsumer.remove(task) || lowPriorityConsumer.remove(task);
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    doSchedule(task, delayInMillis, defaultPriority);
  }

  /**
   * Constructs a {@link OneTimeTaskWrapper} and adds it to the most efficent queue.  If there is 
   * no delay it will use {@link #addToExecuteQueue(OneTimeTaskWrapper)}, if there is a delay it 
   * will be added to {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task Runnable to be executed
   * @param delayInMillis delay to wait before task is run
   * @param priority Priority for task execution
   */
  protected void doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    OneTimeTaskWrapper taskWrapper = new OneTimeTaskWrapper(task, delayInMillis);
    if (delayInMillis == 0) {
      addToExecuteQueue(priority, taskWrapper);
    } else {
      addToScheduleQueue(priority, taskWrapper);
    }
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
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
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }

    doSchedule(task, delayInMs, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task, result);
    doSchedule(rf, delayInMs, priority);
    
    return rf;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task);
    doSchedule(rf, delayInMs, priority);
    
    return rf;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, null);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, 
                                     long recurringDelay, TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    if (priority == null) {
      priority = defaultPriority;
    }

    addToScheduleQueue(priority, new RecurringDelayTaskWrapper(task, getQueueManager(priority), 
                                                               initialDelay, recurringDelay));
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduleAtFixedRate(task, initialDelay, period, null);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period, 
                                  TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    if (priority == null) {
      priority = defaultPriority;
    }

    addToScheduleQueue(priority, new RecurringRateTaskWrapper(task, getQueueManager(priority), 
                                                              initialDelay, period));
  }
  
  /**
   * Simple class to return the appropriate {@link QueueManager} for a given priority.
   * 
   * @param priority Priority to check against for finding the queue
   * @return Task queue which makes sense for the given priority 
   */
  protected QueueManager getQueueManager(TaskPriority priority) {
    switch (priority) {
      case High:
        return highPriorityConsumer;
      case Low:
        return lowPriorityConsumer;
      default:
        throw new UnsupportedOperationException();
    }
  }
  
  /**
   * Adds the ready TaskWrapper to the correct execute queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * 
   * If this is a scheduled or recurring task use {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToExecuteQueue(TaskPriority priority, OneTimeTaskWrapper task) {
    if (workerPool.isShutdownStarted()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    getQueueManager(priority).addExecute(task);
  }
  
  /**
   * Adds the ready TaskWrapper to the correct schedule queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * 
   * If this is just a single execution with no delay use {@link #addToExecuteQueue(OneTimeTaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToScheduleQueue(TaskPriority priority, TaskWrapper task) {
    if (workerPool.isShutdownStarted()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    getQueueManager(priority).addScheduled(task);
  }
  
  @Override
  protected void finalize() {
    // shutdown the thread pool so we don't leak threads if garbage collected
    shutdown();
  }
  
  /**
   * <p>Class to manage the pool of worker threads.  This class handles creating workers, storing 
   * them, and killing them once they are ready to expire.  It also handles finding the 
   * appropriate worker when a task is ready to be executed.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.5.0
   */
  protected static class WorkerPool {
    protected final ThreadFactory threadFactory;
    protected final Object poolSizeChangeLock;
    protected final Object workersLock;
    protected final Deque<Worker> availableWorkers;        // is locked around workersLock
    protected long lastHighDelayMillis;   // is locked around workersLock
    private int waitingForWorkerCount;  // is locked around workersLock
    private int currentPoolSize;  // is locked around workersLock
    private final AtomicBoolean shutdownStarted;
    private volatile boolean shutdownFinishing; // once true, never goes to false
    private volatile long maxWaitForLowPriorityInMs;
    private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
    
    protected WorkerPool(ThreadFactory threadFactory, int poolSize, long maxWaitForLowPriorityInMs) {
      ArgumentVerifier.assertGreaterThanZero(poolSize, "poolSize");
      if (threadFactory == null) {
        threadFactory = new ConfigurableThreadFactory(PriorityScheduler.class.getSimpleName() + "-", true);
      }
      
      poolSizeChangeLock = new Object();
      workersLock = new Object();
      availableWorkers = new ArrayDeque<Worker>(poolSize);
      waitingForWorkerCount = 0;
      currentPoolSize = 0;
      
      //calls to verify and set values
      setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
      
      this.threadFactory = threadFactory;
      this.maxPoolSize = poolSize;
      shutdownStarted = new AtomicBoolean(false);
      shutdownFinishing = false;
      lastHighDelayMillis = 0;
    }

    /**
     * Checks if the shutdown has started by an invocation of {@link #startShutdown()}.
     * 
     * @return {@code true} if the shutdown has started
     */
    public boolean isShutdownStarted() {
      return shutdownStarted.get();
    }

    /**
     * Will start the shutdown of the worker pool.
     * 
     * @return {@code true} if this call initiates the shutdown, {@code false} if the shutdown has already started
     */
    public boolean startShutdown() {
      return ! shutdownStarted.getAndSet(true);
    }
  
    /**
     * Check weather the shutdown process is finished.  In order for the shutdown to finish 
     * {@link #finishShutdown()} must have been invoked. 
     * 
     * @return {@code true} if the scheduler is finishing its shutdown
     */
    public boolean isShutdownFinished() {
      return shutdownFinishing;
    }

    /**
     * Finishes the shutdown of the worker pool.  This will ensure all finishing workers are 
     * killed.
     */
    public void finishShutdown() {
      shutdownFinishing = true;
      
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
     * Getter for the currently set max worker pool size.
     * 
     * @return current max pool size
     */
    public int getMaxPoolSize() {
      return maxPoolSize;
    }

    /**
     * Change the set core pool size.  If the value is less than the current max pool size, the max 
     * pool size will also be updated to this value.
     * 
     * If this was a reduction from the previous value, this call will examine idle workers to see 
     * if they should be expired.  If this call reduced the max pool size, and the current running 
     * thread count is higher than the new max size, this call will NOT block till the pool is 
     * reduced.  Instead as those workers complete, they will clean up on their own.
     * 
     * @param newPoolSize New core pool size, must be at least one
     */
    public void setPoolSize(int newPoolSize) {
      ArgumentVerifier.assertGreaterThanZero(newPoolSize, "newPoolSize");
      
      synchronized (poolSizeChangeLock) {
        boolean poolSizeIncrease = newPoolSize > this.maxPoolSize;
        
        this.maxPoolSize = newPoolSize;
  
        synchronized (workersLock) {
          if (poolSizeIncrease) {
            // now that pool size increased, start any workers we can for the waiting tasks
            if (waitingForWorkerCount > 0) {
              while (availableWorkers.size() < waitingForWorkerCount && 
                     currentPoolSize <= newPoolSize) {
                availableWorkers.add(makeNewWorker());
              }
              
              workersLock.notifyAll();
            }
          } else {
            while (currentPoolSize > newPoolSize && ! availableWorkers.isEmpty()) {
              Worker w = availableWorkers.removeLast();
              killWorker(w);
            }
          }
        }
      }
    }

    /**
     * Getter for the current quantity of workers constructed (either running or idle).
     * 
     * @return current worker count
     */
    public int getCurrentPoolSize() {
      synchronized (workersLock) {
        return currentPoolSize;
      }
    }

    /**
     * Call to check how many workers are currently executing tasks.
     * 
     * @return current number of workers executing tasks
     */
    public int getCurrentRunningCount() {
      synchronized (workersLock) {
        return currentPoolSize - availableWorkers.size();
      }
    }

    /**
     * Ensures all threads have been started.  This will make new idle workers to accept tasks.
     */
    public void prestartAllThreads() {
      synchronized (workersLock) {
        boolean startedThreads = false;
        while (currentPoolSize < maxPoolSize) {
          availableWorkers.addFirst(makeNewWorker());
          startedThreads = true;
        }
        
        if (startedThreads) {
          workersLock.notifyAll();
        }
      }
    }

    /**
     * Getter for the maximum amount of time a low priority task will wait for an available worker.
     * 
     * @return currently set max wait for low priority task
     */
    public long getMaxWaitForLowPriority() {
      return maxWaitForLowPriorityInMs;
    }

    /**
     * Changes the max wait time for an idle worker for low priority tasks.  Changing this will only 
     * take effect for future low priority tasks, it will have no impact for the current low priority 
     * task attempting to get a worker.
     * 
     * @param maxWaitForLowPriorityInMs new time to wait for a thread in milliseconds
     */
    public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
      ArgumentVerifier.assertNotNegative(maxWaitForLowPriorityInMs, "maxWaitForLowPriorityInMs");
      
      this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
    }
  
    /**
     * This function REQUIRES that workersLock is synchronized before calling.  It returns an 
     * available worker if it can get one before the wait time expires.  It will never create 
     * a new worker.
     * 
     * @param maxWaitTimeInMs time to wait for a worker to become available
     * @return an available worker, or {@code null} if no worker became available within the maxWaitTimeInMs
     * @throws InterruptedException Thrown if thread is interrupted while waiting for worker
     */
    protected Worker getExistingWorker(long maxWaitTimeInMs) throws InterruptedException {
      long startTime = -1;
      waitingForWorkerCount++;
      try {
        long waitTime = maxWaitTimeInMs;
        while (availableWorkers.isEmpty() && waitTime > 0) {
          if (waitTime == Long.MAX_VALUE) {  // prevent overflow
            workersLock.wait();
          } else {
            if (startTime < 0) {
              // only set the start time at the first run
              startTime = Clock.accurateForwardProgressingMillis();
            } else {
              long elapsedTime = Clock.accurateForwardProgressingMillis() - startTime;
              waitTime = maxWaitTimeInMs - elapsedTime;
            }
            if (waitTime > 0) {
              workersLock.wait(waitTime);
            }
          }
        }
        
        // always return from the front, if this returns null it's because we exceeded our wait time
        return availableWorkers.pollFirst();
      } finally {
        waitingForWorkerCount--;
      }
    }
    
    /**
     * This function REQUIRES that workersLock is synchronized before calling.  This call creates 
     * a new worker, starts it, but does NOT add it as an available worker (so you can immediately 
     * use it).  If you want this worker to be available for other tasks, it must be added to the 
     * {@code availableWorkers} queue.
     * 
     * @return Newly created worker, started and ready to accept work
     */
    protected Worker makeNewWorker() {
      Worker w = new Worker(this, threadFactory);
      currentPoolSize++;
      w.start();
      
      // will be added to available workers when done with first task
      return w;
    }
    
    /**
     * Shuts down the worker and ensures this now dead worker wont be used.
     * 
     * @param w worker to shutdown
     */
    protected void killWorker(Worker w) {
      // IMPORTANT** if this lock is removed, it is important to read the comment bellow
      synchronized (workersLock) {
        /* this will throw an exception if the worker has already stopped, 
         * we want to ensure the pool size is not decremented more than once for a given worker.
         * 
         * We are able to stop first, because we are holding the workers lock.  In the future if we 
         * try to reduce locking around here, we need to ensure that the worker is removed from the 
         * available workers BEFORE stopping.
         */
        w.stopIfRunning();
        currentPoolSize--;
        // it may not always be here, but it sometimes can (for example when a worker is interrupted)
        availableWorkers.remove(w);
      }
    }
    
    /**
     * Called by the worker after it completes a task.  This is so that we can run any after task 
     * cleanup, and make sure that the worker is now available for future tasks.
     * 
     * @param worker worker that is now idle and ready for more tasks
     */
    protected void workerDone(Worker worker) {
      synchronized (workersLock) {
        if (shutdownFinishing || currentPoolSize > maxPoolSize) {
          killWorker(worker);
        } else {
          // always add to the front so older workers are at the back
          availableWorkers.addFirst(worker);
              
          workersLock.notify();
        }
      }
    }
    
    /**
     * Should be called from a high priority queue consumer when there are no tasks currently 
     * ready for execution.  This should be called before it blocks to wait for new tasks.  It 
     * allows the {@link WorkerPool} to optimize task distribution to know that there are not 
     * any currently ready tasks, and how long until the next one is expected.
     * 
     * @param queueManager QueueManager that is about to block waiting for a task to run
     * @param delayEstimateInMs How long till next task, or {@code Long.MAX_VALUE} if none are queued
     */
    protected void handleEstimatedTimeTillNextTask(QueueManager queueManager, long delayEstimateInMs) {
      if (queueManager.queuePriority == TaskPriority.High) {
        if (delayEstimateInMs > lastHighDelayMillis) {
          lastHighDelayMillis = 0;
        }
      }
    }
  
    /**
     * After a task has been pulled from the queue and is ready to execute it is provided here.  
     * This function will get an available worker (or create one if necessary and possible), and 
     * then provide the task to that available worker.
     * 
     * @param task Task to execute once we have an available worker
     * @throws InterruptedException Thrown if thread is interrupted while waiting for a worker
     */
    protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
      Worker w = null;
      synchronized (workersLock) {
        if (! shutdownFinishing) {
          if (currentPoolSize >= maxPoolSize) {
            // set this value initially so that incoming low priority tasks will recognized the needed delay
            lastHighDelayMillis = task.getDelayEstimateInMs();
            // we can't make the pool any bigger
            w = getExistingWorker(Long.MAX_VALUE);
            // we re-set this value to include the time it took to get a thread
            lastHighDelayMillis = task.getDelayEstimateInMs();
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
        w.nextTask(task);
      }
    }
  
    /**
     * After a task has been pulled from the queue and is ready to execute it is provided here.  
     * This function will get an available worker, waiting a bit of time for one to become 
     * available if none are immediately available.  If after that there is still none available it 
     * will create one (assuming we have not reached our max pool size).  Then the acquired worker 
     * will be provided the task to execute.
     * 
     * @param task Task to execute once we have an available worker
     * @throws InterruptedException Thrown if thread is interrupted while waiting for a worker
     */
    protected void runLowPriorityTask(TaskWrapper task) throws InterruptedException {
      Worker w = null;
      synchronized (workersLock) {
        // wait for high priority tasks that have been waiting longer than us if all workers are consumed
        long waitMs;
        while (currentPoolSize >= maxPoolSize && 
            availableWorkers.size() < WORKER_CONTENTION_LEVEL &&   // only care if there is worker contention
            ! shutdownFinishing &&
            (waitMs = task.getDelayEstimateInMs() - lastHighDelayMillis) > LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS) {
          workersLock.wait(waitMs);
          Clock.systemNanoTime(); // update for getDelayEstimateInMillis
        }
        
        if (! shutdownFinishing) {
          if (currentPoolSize >= maxPoolSize) {
            w = getExistingWorker(Long.MAX_VALUE);
          } else if (currentPoolSize == 0) {
            // first task is low priority, we obviously wont get any workers if we wait, so just make one
            w = makeNewWorker();
          } else {
            w = getExistingWorker(maxWaitForLowPriorityInMs);
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
  }
  
  /**
   * <p>A service which manages the execute queues.  It runs a task to consume from the queues and 
   * execute those tasks as workers become available.  It also manages the queues as tasks are 
   * added, removed, or rescheduled.</p>
   * 
   * <p>Right now this class has a pretty tight dependency on {@link PriorityScheduler}, which is 
   * why this is an inner class.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.4.0
   */
  protected static class QueueManager extends AbstractService implements Runnable {
    protected final WorkerPool workerPool;
    protected final String threadName;
    protected final ConcurrentLinkedQueue<OneTimeTaskWrapper> executeQueue;
    protected final ConcurrentArrayList<TaskWrapper> scheduleQueue;
    protected final TaskPriority queuePriority;
    protected volatile Thread runningThread;
    
    public QueueManager(WorkerPool workerPool, TaskPriority queuePriority, String threadName) {
      this.workerPool = workerPool;
      this.threadName = threadName;
      this.executeQueue = new ConcurrentLinkedQueue<OneTimeTaskWrapper>();
      this.scheduleQueue = new ConcurrentArrayList<TaskWrapper>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
      this.queuePriority = queuePriority;
      runningThread = null;
    }

    /**
     * Removes a given callable from the internal queues (if it exists).
     * 
     * @param task Callable to search for and remove
     * @return {@code true} if the task was found and removed
     */
    public boolean remove(Callable<?> task) {
      {
        Iterator<? extends TaskWrapper> it = executeQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.task, task) && executeQueue.remove(tw)) {
            tw.cancel();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
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
     * Removes a given Runnable from the internal queues (if it exists).
     * 
     * @param task Runnable to search for and remove
     * @return {@code true} if the task was found and removed
     */
    public boolean remove(Runnable task) {
      {
        Iterator<? extends TaskWrapper> it = executeQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.task, task) && executeQueue.remove(tw)) {
            tw.cancel();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
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
     * Adds a task for immediate execution.  No safety checks are done at this point, the task 
     * will be immediately added and available for consumption.
     * 
     * @param task Task to add to end of execute queue
     */
    public void addExecute(OneTimeTaskWrapper task) {
      executeQueue.add(task);

      handleQueueUpdate();
    }

    /**
     * Adds a task for delayed execution.  No safety checks are done at this point.  This call 
     * will safely find the insertion point in the scheduled queue and insert it into that 
     * queue.
     * 
     * @param task Task to insert into the schedule queue
     */
    public void addScheduled(TaskWrapper task) {
      synchronized (scheduleQueue.getModificationLock()) {
        ClockWrapper.stopForcingUpdate();
        try {
          int index = ListUtils.getInsertionEndIndex(scheduleQueue, task, true);
          
          scheduleQueue.add(index, task);
        } finally {
          ClockWrapper.resumeForcingUpdate();
        }
      }
      
      handleQueueUpdate();
    }

    /**
     * Adds a scheduled task to the end of the scheduled queue.  It is expected that this task is 
     * NOT ready for execution, and will later be moved from invoking 
     * {@link #reschedule(RecurringTaskWrapper)}.
     * 
     * @param task Task to add to end of schedule queue
     */
    public void addScheduledLast(RecurringTaskWrapper task) {
      scheduleQueue.addLast(task);
      // no need to notify since this task wont be ready to run
    }

    /**
     * Call to find and reposition a scheduled task.  It is expected that the task provided has 
     * already been added to the queue (likely from a call to 
     * {@link #addScheduledLast(RecurringTaskWrapper)}).  This call will use 
     * {@link RecurringTaskWrapper#getNextDelayInMillis()} to figure out what the new position 
     * within the queue should be.
     * 
     * @param task Task to find in queue and reposition based off next delay
     */
    public void reschedule(RecurringTaskWrapper task) {
      synchronized (scheduleQueue.getModificationLock()) {
        if (! workerPool.isShutdownStarted()) {
          ClockWrapper.stopForcingUpdate();
          try {
            long nextDelay = task.getNextDelayInMillis();
            int insertionIndex = ListUtils.getInsertionEndIndex(scheduleQueue, nextDelay, true);
            
            scheduleQueue.reposition(task, insertionIndex, true);
          } finally {
            ClockWrapper.resumeForcingUpdate();
          }
        }
      }
      
      // need to unpark even if the task is not ready, otherwise we may get stuck on an infinite park
      handleQueueUpdate();
    }

    /**
     * Called to check if either queue has anything to run.  This is just if the queues are empty.  
     * If there are scheduled tasks queued, but not ready to run, this will still return 
     * {@code false}.
     * 
     * @return {@code true} if there are no tasks in either queue
     */
    public boolean isQueueEmpty() {
      return executeQueue.isEmpty() && scheduleQueue.isEmpty();
    }

    /**
     * Call to get the total quantity of tasks within both stored queues.  If you can, 
     * {@link #isQueueEmpty()} is a more efficient call.  This returns the total amount of items 
     * in both the execute and scheduled queue.  If there are scheduled tasks which are NOT ready 
     * to run, they will still be included in this total.
     * 
     * @return Total quantity of tasks queued
     */
    public int queueSize() {
      return scheduleQueue.size() + executeQueue.size();
    }

    public void stopAndDrainQueueInto(List<Runnable> removedTasks) {
      stopIfRunning();
      
      clearQueue(executeQueue, removedTasks);
      synchronized (scheduleQueue.getModificationLock()) {
        clearQueue(scheduleQueue, removedTasks);
      }
    }
  
    private static void clearQueue(Collection<? extends TaskWrapper> queue, List<Runnable> resultList) {
      Iterator<? extends TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        tw.cancel();
        if (resultList != null && ! (tw.task instanceof ShutdownRunnable)) {
          resultList.add(tw.task);
        }
      }
      queue.clear();
    }

    @Override
    protected void startupService() {
      runningThread = workerPool.threadFactory.newThread(this);
      if (runningThread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      runningThread.setDaemon(true);
      runningThread.setName(threadName);
      runningThread.start();
    }

    @Override
    protected void shutdownService() {
      Thread runningThread = this.runningThread;
      this.runningThread = null;
      runningThread.interrupt();
    }
    
    /**
     * Called when the queue has been updated and we may need to wake up the consumer thread.
     */
    protected void handleQueueUpdate() {
      if (! startIfNotStarted()) {
        Thread currRunningThread = runningThread;
        if (currRunningThread != null) {
          LockSupport.unpark(currRunningThread);
        }
      }
    }

    @Override
    public void run() {
      while (runningThread != null) {
        try {
          TaskWrapper nextTask = getNextTask();
          if (nextTask != null) {
            switch (queuePriority) {
              case High:
                workerPool.runHighPriorityTask(nextTask);
                break;
              case Low:
                workerPool.runLowPriorityTask(nextTask);
                break;
              default:
                throw new UnsupportedOperationException();
            }
          }
        } catch (InterruptedException e) {
          stopIfRunning();
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        }
      }
    }
    
    protected TaskWrapper getNextTask() throws InterruptedException {
      while (runningThread != null) {  // loop till we have something to return
        TaskWrapper nextScheduledTask = scheduleQueue.peekFirst();
        TaskWrapper nextExecuteTask = executeQueue.peek();
        if (nextExecuteTask != null) {
          if (nextScheduledTask != null) {
            long scheduleDelay;
            long executeDelay;
            ClockWrapper.stopForcingUpdate();
            try {
              scheduleDelay = nextScheduledTask.getDelay(TimeUnit.MILLISECONDS);
              executeDelay = nextExecuteTask.getDelay(TimeUnit.MILLISECONDS);
            } finally {
              ClockWrapper.resumeForcingUpdate();
            }
            if (scheduleDelay < executeDelay) {
              synchronized (scheduleQueue.getModificationLock()) {
                // scheduled tasks must be removed, and call .executing() while holding the lock
                if (scheduleQueue.remove(nextScheduledTask)) {
                  nextScheduledTask.executing();
                  return nextScheduledTask;
                }
              }
            } else if (executeQueue.remove(nextExecuteTask)) {
              // if we can remove the task (aka it has not been removed already), we can execute it
              nextExecuteTask.executing();
              return nextExecuteTask;
            }
          } else if (executeQueue.remove(nextExecuteTask)) {
            // if we can remove the task (aka it has not been removed already), we can execute it
            nextExecuteTask.executing();
            return nextExecuteTask;
          }
        } else if (nextScheduledTask != null) {
          if (nextScheduledTask.getDelay(TimeUnit.MILLISECONDS) <= 0) {
            synchronized (scheduleQueue.getModificationLock()) {
              // scheduled tasks must be removed, and call .executing() while holding the lock
              if (scheduleQueue.remove(nextScheduledTask)) {
                nextScheduledTask.executing();
                return nextScheduledTask;
              }
            }
          } else {
            workerPool.handleEstimatedTimeTillNextTask(this, nextScheduledTask.getDelayEstimateInMs());
            LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextScheduledTask.getDelay(TimeUnit.MILLISECONDS));
          }
        } else {
          workerPool.handleEstimatedTimeTillNextTask(this, Long.MAX_VALUE);
          LockSupport.park();
        }
        
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }
      }
      
      return null;
    }
  }
  
  /**
   * <p>Runnable which will run on pool threads.  It accepts runnables to run, and tracks 
   * usage.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  // if functions are added here, we need to add them to the overriding wrapper in StrictPriorityScheduler
  protected static class Worker extends AbstractService implements Runnable {
    protected final WorkerPool workerPool;
    protected final Thread thread;
    protected volatile Runnable nextTask;
    
    protected Worker(WorkerPool workerPool, ThreadFactory threadFactory) {
      this.workerPool = workerPool;
      thread = threadFactory.newThread(this);
      if (thread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      nextTask = null;
    }

    @Override
    protected void startupService() {
      thread.start();
    }

    @Override
    protected void shutdownService() {
      LockSupport.unpark(thread);
    }
    
    /**
     * Supply the worker with the next task to run.  It is expected that the worker has been 
     * started before it is provided any tasks.  It must also be complete with it's previous 
     * task before it can be provided another one to run (there is no queuing within the workers).
     * 
     * @param task Task to run on this workers thread
     */
    public void nextTask(Runnable task) {
      nextTask = task;

      LockSupport.unpark(thread);
    }
    
    /**
     * Used internally by the worker to block it's internal thread until another task is provided 
     * to it.
     */
    private void blockTillNextTask() {
      boolean checkedInterrupted = false;
      while (nextTask == null && isRunning()) {
        LockSupport.park(this);

        checkInterrupted();
        checkedInterrupted = true;
      }
      
      if (! checkedInterrupted) {
        // must verify thread is not in interrupted status before it runs a task
        checkInterrupted();
      }
    }
    
    /**
     * Checks the interrupted status of the workers thread.  If it is interrupted the status will 
     * be cleared (unless the pool is shutting down, in which case we will gracefully shutdown the 
     * worker).
     */
    private void checkInterrupted() {
      if (Thread.interrupted()) { // check and clear interrupt
        if (workerPool.isShutdownFinished()) {
          /* If provided a new task, by the time killWorker returns we will still run that task 
           * before letting the thread return.
           */
          workerPool.killWorker(this);
        }
      }
    }
    
    @Override
    public void run() {
      // will break in finally block if shutdown
      while (true) {
        blockTillNextTask();
        
        if (nextTask != null) {
          ExceptionUtils.runRunnable(nextTask);
          nextTask = null;
        }
        // once done handling task
        if (isRunning()) {
          // only check if still running, otherwise worker has already been killed
          workerPool.workerDone(this);
        } else {
          break;
        }
      }
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
    protected final Runnable task;
    protected volatile boolean canceled;
    
    public TaskWrapper(Runnable task) {
      this.task = task;
      canceled = false;
    }
    
    /**
     * Attempts to cancel the task from running (assuming it has not started yet).
     */
    public void cancel() {
      canceled = true;
      
      if (task instanceof Future<?>) {
        ((Future<?>)task).cancel(false);
      }
    }
    
    /**
     * Called as the task is being removed from the queue to prepare for execution.
     */
    public void executing() {
      // nothing by default, override to handle
    }
    
    /**
     * Similar to getDelay, except this implementation is an estimate.  It is only in 
     * milliseconds, and having some slight inaccuracy is not an issue.
     * 
     * @return time in milliseconds till task is ready to run
     */
    protected abstract long getDelayEstimateInMs();
    
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
    protected final long runTime;
    
    protected OneTimeTaskWrapper(Runnable task, long delay) {
      super(task);
      
      runTime = Clock.accurateForwardProgressingMillis() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(runTime - ClockWrapper.getSemiAccurateMillis(), TimeUnit.MILLISECONDS);
    }
    
    @Override
    protected long getDelayEstimateInMs() {
      return runTime - Clock.lastKnownForwardProgressingMillis();
    }

    @Override
    public void run() {
      if (! canceled) {
        task.run();
      }
    }
  }

  /**
   * <p>Abstract wrapper for any tasks which run repeatedly.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected abstract static class RecurringTaskWrapper extends TaskWrapper {
    protected final QueueManager queueManager;
    protected volatile boolean executing;
    protected long nextRunTime;
    
    protected RecurringTaskWrapper(Runnable task, QueueManager queueManager, long initialDelay) {
      super(task);
      
      this.queueManager = queueManager;
      executing = false;
      this.nextRunTime = Clock.accurateForwardProgressingMillis() + initialDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      if (executing) {
        return Long.MAX_VALUE;
      } else {
        return unit.convert(getNextDelayInMillis(), TimeUnit.MILLISECONDS);
      }
    }
    
    /**
     * Checks what the delay time is till the next execution.
     *  
     * @return time in milliseconds till next execution
     */
    protected long getNextDelayInMillis() {
      return nextRunTime - ClockWrapper.getSemiAccurateMillis();
    }
    
    @Override
    protected long getDelayEstimateInMs() {
      return nextRunTime - Clock.lastKnownForwardProgressingMillis();
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
      queueManager.addScheduledLast(this);
    }
    
    /**
     * Called when the implementing class should update the variable {@code nextRunTime} to be the 
     * next absolute time in milliseconds the task should run.
     */
    protected abstract void updateNextRunTime();
    
    /**
     * After the task has completed, this will reschedule the task to run again.
     */
    private void reschedule() {
      updateNextRunTime();
      
      // now that nextRunTime has been set, resort the queue
      queueManager.reschedule(this);
      
      executing = false;
    }

    @Override
    public void run() {
      if (canceled) {
        return;
      }
      try {
        task.run();
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
   * <p>Wrapper for tasks which reschedule after completion.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected static class RecurringDelayTaskWrapper extends RecurringTaskWrapper {
    protected final long recurringDelay;
    
    protected RecurringDelayTaskWrapper(Runnable task, QueueManager queueManager, 
                                        long initialDelay, long recurringDelay) {
      super(task, queueManager, initialDelay);
      
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime = Clock.accurateForwardProgressingMillis() + recurringDelay;
    }
  }
  
  /**
   * <p>Wrapper for tasks which run at a fixed period (regardless of execution time).</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected static class RecurringRateTaskWrapper extends RecurringTaskWrapper {
    protected final long period;
    
    protected RecurringRateTaskWrapper(Runnable task, QueueManager queueManager, 
                                       long initialDelay, long period) {
      super(task, queueManager, initialDelay);
      
      this.period = period;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
  }
  
  /**
   * <p>Runnable to be run after tasks already ready to execute.  That way this can be submitted 
   * with a {@link #execute(Runnable)} to ensure that the shutdown is fair for tasks that were 
   * already ready to be run/executed.  Once this runs the shutdown sequence will be finished, and 
   * no remaining asks in the queue can be executed.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class ShutdownRunnable implements Runnable {
    private final WorkerPool wm;
    private final QueueManager lowPriorityConsumer;
    private final QueueManager highPriorityConsumer;
    
    protected ShutdownRunnable(WorkerPool wm, QueueManager lowPriorityConsumer, QueueManager highPriorityConsumer) {
      this.wm = wm;
      this.lowPriorityConsumer = lowPriorityConsumer;
      this.highPriorityConsumer = highPriorityConsumer;
    }
    
    @Override
    public void run() {
      lowPriorityConsumer.stopAndDrainQueueInto(null);
      highPriorityConsumer.stopAndDrainQueueInto(null);
      wm.finishShutdown();
    }
  }
}
