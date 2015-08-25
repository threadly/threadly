package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Executor to run tasks, schedule tasks.  Unlike 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} this scheduled executor's pool size 
 * can shrink if set with a lower value via {@link #setPoolSize(int)}.  It also has the benefit 
 * that you can provide "low priority" tasks.</p>
 * 
 * <p>These low priority tasks will delay their execution if there are other high priority tasks 
 * ready to run, as long as they have not exceeded their maximum wait time.  If they have exceeded 
 * their maximum wait time, and high priority tasks delay time is less than the low priority delay 
 * time, then those low priority tasks will be executed.  What this results in is a task which has 
 * lower priority, but which wont be starved from execution.</p>
 * 
 * <p>Most tasks provided into this pool will likely want to be "high priority", to more closely 
 * match the behavior of other thread pools.  That is why unless specified by the constructor, the 
 * default {@link TaskPriority} is High.</p>
 * 
 * <p>In all conditions, "low priority" tasks will never be starved.  This makes "low priority" 
 * tasks ideal which do regular leanup, or in general anything that must run, but cares little if 
 * there is a 1, or 10 second gap in the execution time.  That amount of tolerance is adjustable 
 * by setting the {@code maxWaitForLowPriorityInMs} either in the constructor, or at runtime via 
 * {@link #setMaxWaitForLowPriority(long)}.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0 (existed since 1.0.0 as PriorityScheduledExecutor)
 */
@SuppressWarnings("deprecation")
public class PriorityScheduler extends AbstractSubmitterScheduler 
                               implements PrioritySchedulerInterface {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = true;
  protected static final int WORKER_CONTENTION_LEVEL = 2; // level at which no worker contention is considered
  protected static final int LOW_PRIORITY_WAIT_TOLLERANCE_IN_MS = 2;
  // tuned for performance of scheduled tasks
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  protected final WorkerPool workerPool;
  protected final TaskPriority defaultPriority;
  protected final QueueManager taskConsumer;

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This constructs a default priority of high (which makes sense for most use cases).  It 
   * also defaults low priority task wait as 500ms.  It also defaults to all newly created threads 
   * to being daemon threads.
   * 
   * @param poolSize Thread pool size that should be maintained
   */
  public PriorityScheduler(int poolSize) {
    this(poolSize, null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, DEFAULT_NEW_THREADS_DAEMON);
  }
  
  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This constructs a default priority of high (which makes sense for most use cases).  It 
   * also defaults low priority task wait as 500ms.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PriorityScheduler(int poolSize, boolean useDaemonThreads) {
    this(poolSize, null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, useDaemonThreads);
  }

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, DEFAULT_NEW_THREADS_DAEMON);
  }

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
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
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority priority to give tasks which do not specify it
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    this(new WorkerPool(threadFactory, poolSize), 
         maxWaitForLowPriorityInMs, defaultPriority);
  }
  
  /**
   * This constructor is designed for extending classes to be able to provide their own 
   * implementation of {@link WorkerPool}.  Ultimately all constructors will defer to this one.
   * 
   * @param workerPool WorkerPool to handle accepting tasks and providing them to a worker for execution
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param defaultPriority Default priority to store in case no priority is provided for tasks
   */
  protected PriorityScheduler(WorkerPool workerPool, long maxWaitForLowPriorityInMs, 
                              TaskPriority defaultPriority) {
    if (defaultPriority == null) {
      defaultPriority = DEFAULT_PRIORITY;
    }
    
    this.workerPool = workerPool;
    this.defaultPriority = defaultPriority;
    taskConsumer = new QueueManager(workerPool, 
                                    "task consumer for " + this.getClass().getSimpleName(), 
                                    maxWaitForLowPriorityInMs);
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
   * Changes the max wait time for low priority tasks.  This is the amount of time that a low 
   * priority task will wait if there are ready to execute high priority tasks.  After a low 
   * priority task has waited this amount of time, it will be executed fairly with high priority 
   * tasks (meaning it will only execute the high priority task if it has been waiting longer than 
   * the low priority task).
   * 
   * @param maxWaitForLowPriorityInMs new wait time in milliseconds for low priority tasks during thread contention
   */
  public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
    taskConsumer.setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
  }
  
  /**
   * Getter for the amount of time a low priority task will wait during thread contention before 
   * it is eligible for execution.
   * 
   * @return currently set max wait for low priority task
   */
  public long getMaxWaitForLowPriority() {
    return taskConsumer.getMaxWaitForLowPriority();
  }
  
  /**
   * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
   * a future point.
   * 
   * @return quantity of tasks waiting execution or scheduled to be executed later
   */
  public int getScheduledTaskCount() {
    return taskConsumer.getScheduledTaskCount();
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
    
    return taskConsumer.getQueueSet(priority).queueSize();
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
      ShutdownRunnable sr = new ShutdownRunnable(workerPool, taskConsumer);
      QueueSet queueSet = taskConsumer.highPriorityQueueSet;
      queueSet.addExecute(new OneTimeTaskWrapper(sr, 1, queueSet.executeQueue));
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
    return taskConsumer.stopAndClearQueue();
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
   * guarantee).  But while this is being invoked, it will reduce the throughput of execution, so 
   * should NOT be used extremely frequently.
   * 
   * For non-recurring tasks using a future and calling {@link Future#cancel(boolean)} can be a 
   * better solution.
   * 
   * @param task The original runnable provided to the executor
   * @return {@code true} if the runnable was found and removed
   */
  @Override
  public boolean remove(Runnable task) {
    return taskConsumer.remove(task);
  }

  /**
   * Removes the callable task from the execution queue.  It is possible for the callable to still 
   * run until this call has returned.
   * 
   * Note that this call has high guarantees on the ability to remove the task (as in a complete 
   * guarantee).  But while this is being invoked, it will reduce the throughput of execution, so 
   * should NOT be used extremely frequently.
   * 
   * For non-recurring tasks using a future and calling {@link Future#cancel(boolean)} can be a 
   * better solution.
   * 
   * @param task The original callable provided to the executor
   * @return {@code true} if the callable was found and removed
   */
  @Override
  public boolean remove(Callable<?> task) {
    return taskConsumer.remove(task);
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
    QueueSet queueSet = taskConsumer.getQueueSet(priority);
    if (delayInMillis == 0) {
      addToExecuteQueue(queueSet, new OneTimeTaskWrapper(task, delayInMillis, queueSet.executeQueue));
    } else {
      addToScheduleQueue(queueSet, new OneTimeTaskWrapper(task, delayInMillis, queueSet.scheduleQueue));
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

    QueueSet queueSet = taskConsumer.getQueueSet(priority);
    addToScheduleQueue(queueSet, new RecurringDelayTaskWrapper(task, queueSet, 
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

    QueueSet queueSet = taskConsumer.getQueueSet(priority);
    addToScheduleQueue(queueSet, new RecurringRateTaskWrapper(task, queueSet, 
                                                              initialDelay, period));
  }
  
  /**
   * Adds the ready TaskWrapper to the correct execute queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * 
   * If this is a scheduled or recurring task use {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToExecuteQueue(QueueSet queueSet, OneTimeTaskWrapper task) {
    if (workerPool.isShutdownStarted()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    queueSet.addExecute(task);
  }
  
  /**
   * Adds the ready TaskWrapper to the correct schedule queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * 
   * If this is just a single execution with no delay use {@link #addToExecuteQueue(OneTimeTaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void addToScheduleQueue(QueueSet queueSet, TaskWrapper task) {
    if (workerPool.isShutdownStarted()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    queueSet.addScheduled(task);
  }
  
  @Override
  protected void finalize() {
    // shutdown the thread pool so we don't leak threads if garbage collected
    shutdown();
  }
  
  /* TODO - Can we make this structure more generic?  The logic is very similar as what is done in 
   * the NoThreadScheduler.  I attempted to do this but it caused us to make a bunch of top level 
   * interfaces (due to the need of creating generics in the QueueSet structure).
   * 
   * Ultimately to do this in a clean way we would probably need to make "TaskWrapper" a generic 
   * implementation as well.
   */
  /**
   * <p>Class to contain structures for both execution and scheduling.  It also contains logic for 
   * how we get and add tasks to this queue.</p>
   * 
   * <p>This allows us to have one structure for each priority.  Each structure determines what is  
   * the next task for a given priority</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.0.0
   */
  protected static class QueueSet {
    protected final Thread runningThread;
    protected final WorkerPool workerPool;
    protected final ConcurrentLinkedQueue<OneTimeTaskWrapper> executeQueue;
    protected final ConcurrentArrayList<TaskWrapper> scheduleQueue;
    
    public QueueSet(Thread runningThread, WorkerPool workerPool) {
      this.runningThread = runningThread;
      this.workerPool = workerPool;
      this.executeQueue = new ConcurrentLinkedQueue<OneTimeTaskWrapper>();
      this.scheduleQueue = new ConcurrentArrayList<TaskWrapper>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
    }

    /**
     * Adds a task for immediate execution.  No safety checks are done at this point, the task 
     * will be immediately added and available for consumption.
     * 
     * @param task Task to add to end of execute queue
     */
    public void addExecute(OneTimeTaskWrapper task) {
      executeQueue.add(task);

      LockSupport.unpark(runningThread);
    }

    /**
     * Adds a task for delayed execution.  No safety checks are done at this point.  This call 
     * will safely find the insertion point in the scheduled queue and insert it into that 
     * queue.
     * 
     * @param task Task to insert into the schedule queue
     */
    public void addScheduled(TaskWrapper task) {
      int insertionIndex;
      synchronized (scheduleQueue.getModificationLock()) {
        insertionIndex = TaskListUtils.getInsertionEndIndex(scheduleQueue, task.getRunTime());
        
        scheduleQueue.add(insertionIndex, task);
      }
      
      if (insertionIndex == 0) {
        LockSupport.unpark(runningThread);
      }
    }

    /**
     * Call to find and reposition a scheduled task.  It is expected that the task provided has 
     * already been added to the queue.  This call will use 
     * {@link RecurringTaskWrapper#getRunTime()} to figure out what the new position within the 
     * queue should be.
     * 
     * @param task Task to find in queue and reposition based off next delay
     */
    public void reschedule(RecurringTaskWrapper task) {
      int insertionIndex = 1;
      synchronized (scheduleQueue.getModificationLock()) {
        if (! workerPool.isShutdownStarted()) {
          insertionIndex = TaskListUtils.getInsertionEndIndex(scheduleQueue, task.getNextRunTime());
          
          scheduleQueue.reposition(task, insertionIndex, true);
        }
      }
      
      // need to unpark even if the task is not ready, otherwise we may get stuck on an infinite park
      if (insertionIndex == 0) {
        LockSupport.unpark(runningThread);
      }
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
     * Call to get the total quantity of tasks within both stored queues.  This returns the total 
     * quantity of items in both the execute and scheduled queue.  If there are scheduled tasks 
     * which are NOT ready to run, they will still be included in this total.
     * 
     * @return Total quantity of tasks queued
     */
    public int queueSize() {
      return executeQueue.size() + scheduleQueue.size();
    }

    public void drainQueueInto(List<TaskWrapper> removedTasks) {
      clearQueue(executeQueue, removedTasks);
      synchronized (scheduleQueue.getModificationLock()) {
        clearQueue(scheduleQueue, removedTasks);
      }
    }
  
    private static void clearQueue(Collection<? extends TaskWrapper> queue, List<TaskWrapper> resultList) {
      Iterator<? extends TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        tw.cancel();
        if (! (tw.task instanceof ShutdownRunnable)) {
          int index = TaskListUtils.getInsertionEndIndex(resultList, tw.getRunTime());
          resultList.add(index, tw);
        }
      }
      queue.clear();
    }
    
    /**
     * Gets the next task from this {@link QueueSet}.  This inspects both the execute queue and 
     * against scheduled tasks to determine which task in this {@link QueueSet} should be executed 
     * next.
     * 
     * The task returned from this may not be ready to executed, but at the time of calling it 
     * will be the next one to execute.
     * 
     * @return TaskWrapper which will be executed next, or {@code null} if there are no tasks
     */
    public TaskWrapper getNextTask() {
      TaskWrapper scheduledTask = scheduleQueue.peekFirst();
      TaskWrapper executeTask = executeQueue.peek();
      if (executeTask != null) {
        if (scheduledTask != null) {
          if (scheduledTask.getRunTime() < executeTask.getRunTime()) {
            return scheduledTask;
          } else {
            return executeTask;
          }
        } else {
          return executeTask;
        }
      } else {
        return scheduledTask;
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
    protected final QueueSet highPriorityQueueSet;
    protected final QueueSet lowPriorityQueueSet;
    protected volatile Thread runningThread;
    private volatile long maxWaitForLowPriorityInMs;
    
    public QueueManager(WorkerPool workerPool, String threadName, 
                        long maxWaitForLowPriorityInMs) {
      this.workerPool = workerPool;
      runningThread = workerPool.threadFactory.newThread(this);
      if (runningThread.isAlive()) {
        runningThread = null;
        throw new IllegalThreadStateException();
      }
      runningThread.setDaemon(true);
      runningThread.setName(threadName);
      this.highPriorityQueueSet = new QueueSet(runningThread, workerPool);
      this.lowPriorityQueueSet = new QueueSet(runningThread, workerPool);
      
      // call to verify and set values
      setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
      
      start();
    }
    
    /**
     * Returns the {@link QueueSet} for a specified priority.
     * 
     * @param priority Priority that should match to the given {@link QueueSet}
     * @return {@link QueueSet} which matches to the priority
     */
    public QueueSet getQueueSet(TaskPriority priority) {
      if (priority == TaskPriority.High) {
        return highPriorityQueueSet;
      } else {
        return lowPriorityQueueSet;
      }
    }

    /**
     * Stops the thread which is consuming tasks and providing them to {@link Worker}'s.  If we 
     * already have an idle worker, that worker will be returned to the WorkerPool.
     * 
     * Once stopped, the tasks remaining in queue will be cleared, and returned.
     * 
     * @return List of runnables that were waiting in queue, in an approximate order of execution
     */
    public List<Runnable> stopAndClearQueue() {
      stopIfRunning();

      List<TaskWrapper> wrapperList = new ArrayList<TaskWrapper>(getScheduledTaskCount());
      highPriorityQueueSet.drainQueueInto(wrapperList);
      lowPriorityQueueSet.drainQueueInto(wrapperList);
      
      return ContainerHelper.getContainedRunnables(wrapperList);
    }

    @Override
    protected void startupService() {
      runningThread.start();
    }

    @Override
    protected void shutdownService() {
      Thread runningThread = this.runningThread;
      this.runningThread = null;
      runningThread.interrupt();
    }

    @Override
    public void run() {
      Worker worker = null;
      while (runningThread != null) {
        try {
          worker = workerPool.getWorker();
          if (worker != null) {
            TaskWrapper nextTask = getNextReadyTask();
            if (nextTask != null) {
              worker.nextTask(nextTask);
            } else {
              workerPool.workerDone(worker);
            }
          }
        } catch (InterruptedException e) {
          if (worker != null) {
            workerPool.workerDone(worker);
          }
          stopIfRunning();
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          if (worker != null) {
            // we kill the worker here because this condition is not expected
            workerPool.killWorker(worker);
          }
          ExceptionUtils.handleException(t);
        } finally {
          worker = null;
        }
      }
    }
    
    /**
     * Gets the next task that is ready to execute immediately.  This call gets the next high and 
     * low priority task, compares them and determines which one should be executed next.
     * 
     * If there are no tasks ready to be executed this will block until one becomes available 
     * and ready to execute.
     * 
     * @return Task to be executed next, or {@code null} if shutdown while waiting for task
     * @throws InterruptedException Thrown if calling thread is interrupted while waiting for a task
     */
    protected TaskWrapper getNextReadyTask() throws InterruptedException {
      while (runningThread != null) {  // loop till we have something to return
        TaskWrapper nextHighPriorityTask = highPriorityQueueSet.getNextTask();
        TaskWrapper nextLowPriorityTask = lowPriorityQueueSet.getNextTask();
        if (nextLowPriorityTask == null) {
          if (nextHighPriorityTask == null) {
            // no tasks, so just wait till any tasks come in
            LockSupport.park();
          } else {
            // only high priority task is queued, so run if ready
            long nextTaskDelay = nextHighPriorityTask.getScheduleDelay();
            if (nextTaskDelay > 0) {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextTaskDelay);
            } else if (nextHighPriorityTask.canExecute()) {
              return nextHighPriorityTask;
            }
          }
        } else if (nextHighPriorityTask == null) {
          // only low priority task is queued, so run if ready
          long nextTaskDelay = nextLowPriorityTask.getScheduleDelay();
          if (nextTaskDelay > 0) {
            LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextTaskDelay);
          } else if (nextLowPriorityTask.canExecute()) {
            return nextLowPriorityTask;
          }
        } else if (nextHighPriorityTask.getRunTime() <= nextLowPriorityTask.getRunTime()) {
          // through cheap check we can see the high priority has been waiting longer
          long nextTaskDelay = nextHighPriorityTask.getScheduleDelay();
          if (nextTaskDelay > 0) {
            LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextTaskDelay);
          } else if (nextHighPriorityTask.canExecute()) {
            return nextHighPriorityTask;
          }
        } else {
          // both tasks are available, so see which one makes sense to run
          long now = Clock.accurateForwardProgressingMillis();
          // at this point we know nextHighDelay > nextLowDelay due to check above
          long nextHighDelay = nextHighPriorityTask.getRunTime() - now;
          long nextLowDelay = nextLowPriorityTask.getRunTime() - now;
          if (nextHighDelay <= 0) {
            /* high task is ready, make sure it should be picked...we will pick the low priority task 
             * if it has been waiting longer than the high, and if the low priority has been waiting at 
             * least the max wait time...otherwise we favor the high priority task
             */
            if (nextLowDelay < maxWaitForLowPriorityInMs * -1) {
              if (nextLowPriorityTask.canExecute()) {
                return nextLowPriorityTask;
              }
            } else if (nextHighPriorityTask.canExecute()) {
              return nextHighPriorityTask;
            }
          } else if (nextLowDelay <= 0) {
            if (nextLowPriorityTask.canExecute()) {
              return nextLowPriorityTask;
            }
          } else {
            if (nextLowDelay < nextHighDelay) {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextLowDelay);
            } else {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextHighDelay);
            }
          }
        }
        
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException();
        }
      }
      
      return null;
    }
    
    /**
     * Returns how many tasks are either waiting to be executed, or are scheduled to be executed at 
     * a future point.
     * 
     * @return quantity of tasks waiting execution or scheduled to be executed later
     */
    public int getScheduledTaskCount() {
      return highPriorityQueueSet.queueSize() + lowPriorityQueueSet.queueSize();
    }
    
    /**
     * Removes the runnable task from the execution queue.  It is possible for the runnable to 
     * still run until this call has returned.
     * 
     * Note that this call has high guarantees on the ability to remove the task (as in a complete 
     * guarantee).  But while this is being invoked, it will reduce the throughput of execution, 
     * so should NOT be used extremely frequently.
     * 
     * @param task The original runnable provided to the executor
     * @return {@code true} if the runnable was found and removed
     */
    public boolean remove(Runnable task) {
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task);
    }
    
    /**
     * Removes the callable task from the execution queue.  It is possible for the callable to 
     * still run until this call has returned.
     * 
     * Note that this call has high guarantees on the ability to remove the task (as in a complete 
     * guarantee).  But while this is being invoked, it will reduce the throughput of execution, 
     * so should NOT be used extremely frequently.
     * 
     * @param task The original callable provided to the executor
     * @return {@code true} if the callable was found and removed
     */
    public boolean remove(Callable<?> task) {
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task);
    }
    
    /**
     * Changes the max wait time for low priority tasks.  This is the amount of time that a low 
     * priority task will wait if there are ready to execute high priority tasks.  After a low 
     * priority task has waited this amount of time, it will be executed fairly with high priority 
     * tasks (meaning it will only execute the high priority task if it has been waiting longer than 
     * the low priority task).
     * 
     * @param maxWaitForLowPriorityInMs new wait time in milliseconds for low priority tasks during thread contention
     */
    public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
      ArgumentVerifier.assertNotNegative(maxWaitForLowPriorityInMs, "maxWaitForLowPriorityInMs");
      
      this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
    }
    
    /**
     * Getter for the amount of time a low priority task will wait during thread contention before 
     * it is eligible for execution.
     * 
     * @return currently set max wait for low priority task
     */
    public long getMaxWaitForLowPriority() {
      return maxWaitForLowPriorityInMs;
    }
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
    protected boolean waitingForWorker;  // is locked around workersLock
    protected int currentPoolSize;  // is locked around workersLock
    private final AtomicBoolean shutdownStarted;
    private volatile boolean shutdownFinishing; // once true, never goes to false
    private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
    
    protected WorkerPool(ThreadFactory threadFactory, int poolSize) {
      ArgumentVerifier.assertGreaterThanZero(poolSize, "poolSize");
      if (threadFactory == null) {
        threadFactory = new ConfigurableThreadFactory(PriorityScheduler.class.getSimpleName() + "-", true);
      }
      
      poolSizeChangeLock = new Object();
      workersLock = new Object();
      availableWorkers = new ArrayDeque<Worker>(poolSize);
      waitingForWorker = false;
      currentPoolSize = 0;
      
      this.threadFactory = threadFactory;
      this.maxPoolSize = poolSize;
      shutdownStarted = new AtomicBoolean(false);
      shutdownFinishing = false;
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
            if (waitingForWorker) {
              availableWorkers.add(makeNewWorker());
              
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
        int workerOutQty = currentPoolSize - availableWorkers.size();
        if (waitingForWorker || currentPoolSize == 0) {
          return workerOutQty;
        } else {
          return workerOutQty - 1;
        }
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
     * Call to get (or create, if able to) an idle worker.  If there are no idle workers available 
     * then this call will block until one does become available.
     * 
     * @return A worker ready to accept a task or {@code null} if the pool was shutdown
     * @throws InterruptedException Thrown if this thread is interrupted while waiting to acquire a worker
     */
    public Worker getWorker() throws InterruptedException {
      synchronized (workersLock) {
        while (availableWorkers.isEmpty() && ! shutdownFinishing) {
          if (currentPoolSize < maxPoolSize) {
            return makeNewWorker();
          } else {
            waitingForWorker = true;
            try {
              workersLock.wait();
            } finally {
              waitingForWorker = false;
            }
          }
        }
        
        return availableWorkers.poll();
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
          
          if (waitingForWorker) {
            workersLock.notify();
          }
        }
      }
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
    protected volatile TaskWrapper nextTask;
    
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
    public void nextTask(TaskWrapper task) {
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
          nextTask.runTask();
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
  protected abstract static class TaskWrapper implements DelayedTask, 
                                                         RunnableContainerInterface {
    protected final Runnable task;
    protected volatile boolean canceled;
    
    public TaskWrapper(Runnable task) {
      this.task = task;
      canceled = false;
    }
    
    /**
     * Similar to {@link Runnable#run()}, this is invoked to execute the contained task.  One 
     * critical difference is this implementation should never throw an exception (even 
     * {@link RuntimeException}'s).  Throwing such an exception would result in the worker thread 
     * dying (and being leaked from the pool).
     */
    public abstract void runTask();

    /**
     * Attempts to cancel the task from running (assuming it has not started yet).  If the task is 
     * recurring then future executions will also be avoided.
     */
    public void cancel() {
      canceled = true;
      
      if (task instanceof Future<?>) {
        ((Future<?>)task).cancel(false);
      }
    }
    
    /**
     * Called as the task is being removed from the queue to prepare for execution.
     * 
     * @return true if the task should be executed
     */
    public abstract boolean canExecute();
    
    /**
     * Call to see how long the task should be delayed before execution.  While this may return 
     * either positive or negative numbers, only an accurate number is returned if the task must 
     * be delayed for execution.  If the task is ready to execute it may return zero even though 
     * it is past due.  For that reason you can NOT use this to compare two tasks for execution 
     * order, instead you should use {@link #getRunTime()}.
     * 
     * @return delay in milliseconds till task can be run
     */
    public long getScheduleDelay() {
      if (getRunTime() > Clock.lastKnownForwardProgressingMillis()) {
        return getRunTime() - Clock.accurateForwardProgressingMillis();
      } else {
        return 0;
      }
    }
    
    @Override
    public String toString() {
      return task.toString();
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Wrapper for tasks which only executes once.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class OneTimeTaskWrapper extends TaskWrapper {
    protected final Queue<? extends TaskWrapper> taskQueue;
    protected final long runTime;
    
    protected OneTimeTaskWrapper(Runnable task, long delay, Queue<? extends TaskWrapper> taskQueue) {
      super(task);
      
      this.taskQueue = taskQueue;
      this.runTime = Clock.accurateForwardProgressingMillis() + delay;
    }
    
    @Override
    public long getRunTime() {
      return runTime;
    }

    @Override
    public void runTask() {
      if (! canceled) {
        ExceptionUtils.runRunnable(task);
      }
    }

    @Override
    public boolean canExecute() {
      return taskQueue.remove(this);
    }
  }

  /**
   * <p>Abstract wrapper for any tasks which run repeatedly.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected abstract static class RecurringTaskWrapper extends TaskWrapper {
    protected final QueueSet queueSet;
    protected volatile boolean executing;
    protected long nextRunTime;
    
    protected RecurringTaskWrapper(Runnable task, QueueSet queueSet, long initialDelay) {
      super(task);
      
      this.queueSet = queueSet;
      executing = false;
      this.nextRunTime = Clock.accurateForwardProgressingMillis() + initialDelay;
    }
    
    /**
     * Checks what the delay time is till the next execution.
     *  
     * @return time in milliseconds till next execution
     */
    public long getNextRunTime() {
      return nextRunTime;
    }
    
    @Override
    public long getRunTime() {
      if (executing) {
        return Long.MAX_VALUE;
      } else {
        return nextRunTime;
      }
    }

    @Override
    public boolean canExecute() {
      synchronized (queueSet.scheduleQueue.getModificationLock()) {
        int index = queueSet.scheduleQueue.indexOf(this);
        if (index < 0) {
          return false;
        } else {
          /* we have to reposition to the end atomically so that this task can be removed if 
           * requested to be removed.  We can put it at the end because we know this task wont 
           * run again till it has finished (which it will be inserted at the correct point in 
           * queue then.
           */
          queueSet.scheduleQueue.reposition(index, queueSet.scheduleQueue.size());
          executing = true;
          return true;
        }
      }
    }
    
    /**
     * Called when the implementing class should update the variable {@code nextRunTime} to be the 
     * next absolute time in milliseconds the task should run.
     */
    protected abstract void updateNextRunTime();

    @Override
    public void runTask() {
      if (canceled) {
        return;
      }
      // no need for try/finally due to ExceptionUtils usage
      ExceptionUtils.runRunnable(task);
      if (! canceled) {
        try {
          updateNextRunTime();
          
          // now that nextRunTime has been set, resort the queue
          queueSet.reschedule(this);
          
          executing = false;
        } catch (java.util.NoSuchElementException e) {
          if (canceled) {
            /* this is a possible condition where shutting down 
             * the thread pool occurred while rescheduling the item. 
             * 
             * Since this is unlikely, we just swallow the exception here.
             */
          } else {
            /* This condition however would not be expected, 
             * so we should ensure it's handled.
             */
            ExceptionUtils.handleException(e);
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
    
    protected RecurringDelayTaskWrapper(Runnable task, QueueSet queueSet, 
                                        long initialDelay, long recurringDelay) {
      super(task, queueSet, initialDelay);
      
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
    
    protected RecurringRateTaskWrapper(Runnable task, QueueSet queueSet, 
                                       long initialDelay, long period) {
      super(task, queueSet, initialDelay);
      
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
    private final QueueManager taskConsumer;
    
    protected ShutdownRunnable(WorkerPool wm, QueueManager taskConsumer) {
      this.wm = wm;
      this.taskConsumer = taskConsumer;
    }
    
    @Override
    public void run() {
      taskConsumer.stopAndClearQueue();
      wm.finishShutdown();
    }
  }
}
