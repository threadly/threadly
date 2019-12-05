package org.threadly.concurrent;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * Executor to run tasks, schedule tasks.  Unlike 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} this scheduled executor's pool size 
 * can shrink if set with a lower value via {@link #setPoolSize(int)}.  It also has the benefit 
 * that you can provide "low priority" tasks.
 * <p>
 * These low priority tasks will delay their execution if there are other high priority tasks 
 * ready to run, as long as they have not exceeded their maximum wait time.  If they have exceeded 
 * their maximum wait time, and high priority tasks delay time is less than the low priority delay 
 * time, then those low priority tasks will be executed.  What this results in is a task which has 
 * lower priority, but which wont be starved from execution.
 * <p>
 * Most tasks provided into this pool will likely want to be "high priority", to more closely 
 * match the behavior of other thread pools.  That is why unless specified by the constructor, the 
 * default {@link TaskPriority} is High.
 * <p>
 * In all conditions, "low priority" tasks will never be starved.  This makes "low priority" tasks 
 * ideal which do regular cleanup, or in general anything that must run, but cares little if there 
 * is a 1, or 10 second gap in the execution time.  That amount of tolerance is adjustable by 
 * setting the {@code maxWaitForLowPriorityInMs} either in the constructor, or at runtime via 
 * {@link #setMaxWaitForLowPriority(long)}.
 * 
 * @since 2.2.0 (since 1.0.0 as PriorityScheduledExecutor)
 */
public class PriorityScheduler extends AbstractPriorityScheduler {
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = true;
  protected static final boolean DEFAULT_STARVABLE_STARTS_THREADS = false;
  
  protected final WorkerPool workerPool;
  protected final QueueManager queueManager;

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
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
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
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param useDaemonThreads {@code true} if newly created threads should be daemon
   */
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs, boolean useDaemonThreads) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, DEFAULT_STARVABLE_STARTS_THREADS, 
         new ConfigurableThreadFactory(PriorityScheduler.class.getSimpleName() + "-", 
                                       true, useDaemonThreads, Thread.NORM_PRIORITY, null, null));
  }

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, 
                           long maxWaitForLowPriorityInMs, ThreadFactory threadFactory) {
    this(poolSize, defaultPriority, maxWaitForLowPriorityInMs, 
         DEFAULT_STARVABLE_STARTS_THREADS, threadFactory);
  }

  /**
   * Constructs a new thread pool, though threads will be lazily started as it has tasks ready to 
   * run.  This provides the extra parameters to tune what tasks submitted without a priority 
   * will be scheduled as.  As well as the maximum wait for low priority tasks.
   * 
   * @param poolSize Thread pool size that should be maintained
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   * @param stavableStartsThreads {@code true} to have TaskPriority.Starvable tasks start new threads
   * @param threadFactory thread factory for producing new threads within executor
   */
  public PriorityScheduler(int poolSize, TaskPriority defaultPriority, long maxWaitForLowPriorityInMs, 
                           boolean stavableStartsThreads, ThreadFactory threadFactory) {
    this(new WorkerPool(threadFactory, poolSize, stavableStartsThreads), 
         defaultPriority, maxWaitForLowPriorityInMs);
  }
  
  /**
   * This constructor is designed for extending classes to be able to provide their own 
   * implementation of {@link WorkerPool}.  Ultimately all constructors will defer to this one.
   * 
   * @param workerPool WorkerPool to handle accepting tasks and providing them to a worker for execution
   * @param defaultPriority Default priority to store in case no priority is provided for tasks
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  protected PriorityScheduler(WorkerPool workerPool, TaskPriority defaultPriority, 
                              long maxWaitForLowPriorityInMs) {
    super(defaultPriority);
    
    this.workerPool = workerPool;
    queueManager = new QueueManager(workerPool, maxWaitForLowPriorityInMs);
    
    workerPool.start(queueManager);
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
   * Getter for the current quantity of threads running in this pool (either active or idle).  
   * This is different than the size returned from {@link #getMaxPoolSize()} in that we 
   * lazily create threads.  This represents the amount of threads needed to be created so far, 
   * where {@link #getMaxPoolSize()} represents the amount of threads the pool may grow to.
   * 
   * @return current thread count
   */
  public int getCurrentPoolSize() {
    return workerPool.getCurrentPoolSize();
  }
  
  /**
   * Change the set thread pool size.
   * <p>
   * If the value is less than the current running threads, as threads finish they will exit 
   * rather than accept new tasks.  No currently running tasks will be interrupted, rather we 
   * will just wait for them to finish before killing the thread.
   * <p>
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
   * Adjust the pools size by a given delta.  If the provided delta would result in a pool size 
   * of zero or less, then a {@link IllegalStateException} will be thrown.
   * 
   * @param delta Delta to adjust the max pool size by
   */
  public void adjustPoolSize(int delta) {
    workerPool.adjustPoolSize(delta);
  }
  
  /**
   * Call to check how many tasks are currently being executed in this thread pool.  Unlike 
   * {@link #getCurrentPoolSize()}, this count will NOT include idle threads waiting to execute 
   * tasks.
   * 
   * @return current number of running tasks
   */
  @Override
  public int getActiveTaskCount() {
    return workerPool.getActiveTaskCount();
  }

  /**
   * Ensures all threads have been started, it will create threads till the thread count matches 
   * the set pool size (checked via {@link #getMaxPoolSize()}).  These new threads will remain 
   * idle till there is tasks ready to execute.
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
   * <p>
   * If you wish to not want to run any queued tasks you should use {@link #shutdownNow()}.
   */
  public void shutdown() {
    workerPool.startShutdown();
  }

  /**
   * Stops any new tasks from being able to be executed and removes workers from the pool.
   * <p>
   * This implementation refuses new submissions after this call.  And will NOT interrupt any 
   * tasks which are currently running.  However any tasks which are waiting in queue to be run 
   * (but have not started yet), will not be run.  Those waiting tasks will be removed, and as 
   * workers finish with their current tasks the threads will be joined.
   * 
   * @return List of runnables which were waiting to execute
   */
  public List<Runnable> shutdownNow() {
    workerPool.startShutdown();
    List<Runnable> awaitingTasks = queueManager.clearQueue();
    workerPool.finishShutdown();
    
    return awaitingTasks;
  }
  
  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block forever.
   * 
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public void awaitTermination() throws InterruptedException {
    awaitTermination(Long.MAX_VALUE);
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block until the 
   * timeout is reached.
   * 
   * @param timeoutMillis time to block and wait for thread pool to shutdown
   * @return {@code true} if the pool has shutdown, false if timeout was reached
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
    return workerPool.awaitTermination(timeoutMillis);
  }
  
  @Override
  public int getQueuedTaskCount() {
    // subtract one for hack task for spin issue
    return super.getQueuedTaskCount() - 1;
  }
  
  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    // subtract one from starvable count for hack task for spin issue
    return super.getQueuedTaskCount(priority) - (priority == TaskPriority.Starvable ? 1 : 0);
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    OneTimeTaskWrapper result;
    if (delayInMillis == 0) {
      QueueSet queueSet = queueManager.getQueueSet(priority);
      queueExecute(queueSet, (result = new ImmediateTaskWrapper(task, queueSet.executeQueue)));
      // bellow are optimized for scheduled tasks
    } else if (priority == TaskPriority.High) {
      queueScheduled(queueManager.highPriorityQueueSet, 
                     (result = new AccurateOneTimeTaskWrapper(task, queueManager.highPriorityQueueSet
                                                                                .scheduleQueue, 
                                                              Clock.accurateForwardProgressingMillis() + 
                                                                delayInMillis)));
    } else if (priority == TaskPriority.Low) {
      queueScheduled(queueManager.lowPriorityQueueSet, 
                     (result = new GuessOneTimeTaskWrapper(task, queueManager.lowPriorityQueueSet
                                                                             .scheduleQueue, 
                                                           Clock.accurateForwardProgressingMillis() + 
                                                             delayInMillis)));
    } else {
      queueScheduled(queueManager.starvablePriorityQueueSet, 
                     (result = new GuessOneTimeTaskWrapper(task, queueManager.starvablePriorityQueueSet
                                                                             .scheduleQueue, 
                                                           Clock.accurateForwardProgressingMillis() + 
                                                             delayInMillis)));
    }
    return result;
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

    if (priority == TaskPriority.High) {
      queueScheduled(queueManager.highPriorityQueueSet, 
                     new AccurateRecurringDelayTaskWrapper(task, queueManager.highPriorityQueueSet, 
                                                           Clock.accurateForwardProgressingMillis() + 
                                                             initialDelay, 
                                                           recurringDelay));
    } else if (priority == TaskPriority.Low) {
      queueScheduled(queueManager.lowPriorityQueueSet, 
                     new GuessRecurringDelayTaskWrapper(task, queueManager.lowPriorityQueueSet, 
                                                        Clock.accurateForwardProgressingMillis() + 
                                                          initialDelay, 
                                                        recurringDelay));
    } else {
      queueScheduled(queueManager.starvablePriorityQueueSet, 
                     new GuessRecurringDelayTaskWrapper(task, queueManager.starvablePriorityQueueSet, 
                                                        Clock.accurateForwardProgressingMillis() + 
                                                          initialDelay, 
                                                        recurringDelay));
    }
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

    if (priority == TaskPriority.High) {
      queueScheduled(queueManager.highPriorityQueueSet, 
                     new AccurateRecurringRateTaskWrapper(task, queueManager.highPriorityQueueSet, 
                                                          Clock.accurateForwardProgressingMillis() + 
                                                            initialDelay, 
                                                          period));
    } else if (priority == TaskPriority.Low) {
      queueScheduled(queueManager.lowPriorityQueueSet, 
                     new GuessRecurringRateTaskWrapper(task, queueManager.lowPriorityQueueSet, 
                                                       Clock.accurateForwardProgressingMillis() + 
                                                         initialDelay, 
                                                       period));
    } else {
      queueScheduled(queueManager.starvablePriorityQueueSet, 
                     new GuessRecurringRateTaskWrapper(task, queueManager.starvablePriorityQueueSet, 
                                                       Clock.accurateForwardProgressingMillis() + 
                                                         initialDelay, 
                                                       period));
    }
  }
  
  /**
   * Adds the ready TaskWrapper to the correct execute queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * <p>
   * If this is a scheduled or recurring task use {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void queueExecute(QueueSet queueSet, OneTimeTaskWrapper task) {
    if (workerPool.isShutdownStarted()) {
      throw new RejectedExecutionException("Thread pool shutdown");
    }
    
    queueSet.addExecute(task);
  }
  
  /**
   * Adds the ready TaskWrapper to the correct schedule queue.  Using the priority specified in the 
   * task, we pick the correct queue and add it.
   * <p>
   * If this is just a single execution with no delay use {@link #addToExecuteQueue(OneTimeTaskWrapper)}.
   * 
   * @param task {@link TaskWrapper} to queue for the scheduler
   */
  protected void queueScheduled(QueueSet queueSet, TaskWrapper task) {
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

  @Override
  protected QueueManager getQueueManager() {
    return queueManager;
  }
  
  /**
   * Class to manage the pool of worker threads.  This class handles creating workers, storing 
   * them, and killing them once they are ready to expire.  It also handles finding the 
   * appropriate worker when a task is ready to be executed.
   * 
   * @since 3.5.0
   */
  protected static class WorkerPool implements QueueSetListener {
    protected final ThreadFactory threadFactory;
    protected final boolean stavableStartsThreads;
    protected final Object poolSizeChangeLock;
    protected final Object idleWorkerDequeLock;
    protected final LongAdder idleWorkerCount;
    protected final AtomicReference<Worker> idleWorker;
    protected final AtomicInteger currentPoolSize;
    protected final Object workerStopNotifyLock;
    private final AtomicBoolean shutdownStarted;
    private volatile boolean shutdownFinishing; // once true, never goes to false
    private volatile int maxPoolSize;  // can only be changed when poolSizeChangeLock locked
    private volatile long workerTimedParkRunTime;
    private QueueManager queueManager;  // set before any threads started
    
    public WorkerPool(ThreadFactory threadFactory, int poolSize, boolean stavableStartsThreads) {
      ArgumentVerifier.assertGreaterThanZero(poolSize, "poolSize");
      if (threadFactory == null) {
        threadFactory = new ConfigurableThreadFactory(PriorityScheduler.class.getSimpleName() + "-", true);
      }
      
      poolSizeChangeLock = new Object();
      idleWorkerDequeLock = new Object();
      idleWorkerCount = new LongAdder();
      idleWorker = new AtomicReference<>(null);
      currentPoolSize = new AtomicInteger(0);
      workerStopNotifyLock = new Object();
      
      this.threadFactory = threadFactory;
      this.stavableStartsThreads = stavableStartsThreads;
      this.maxPoolSize = poolSize;
      this.workerTimedParkRunTime = Long.MAX_VALUE;
      shutdownStarted = new AtomicBoolean(false);
      shutdownFinishing = false;
    }

    /**
     * Starts the pool, constructing the first thread to start consuming tasks (and starting other 
     * threads as appropriate).  This should only be called once, and can NOT be called concurrently.
     * 
     * @param queueManager QueueManager to source tasks for execution from
     */
    public void start(QueueManager queueManager) {
      if (currentPoolSize.get() != 0) {
        throw new IllegalStateException();
      }
      
      this.queueManager = queueManager;
      
      // this is to avoid a deficiency in workerIdle that could cause an idle thread to spin.  This 
      // spin would be only if there is only one recurring task, and WHILE that recurring task is 
      // running.  We solve this by adding this recurring task which wont run very long, and is 
      // scheduled to run very infrequently (Using Integer.MAX_VALUE that's every 24 days).
      // we add this directly into the scheduledQueue structure to avoid having handleQueueUpdated 
      // invoked, and thus avoid starting any threads at this point.
      InternalRunnable doNothingRunnable = new InternalRunnable() {
        @Override
        public void run() {
          // nothing added here so that task runs as short as possible
          // must be InternalRunnable, and not DoNothingRunnable so it's hidden from the task queue
        }
      };
      queueManager.starvablePriorityQueueSet
                  .scheduleQueue.add(new GuessRecurringRateTaskWrapper(doNothingRunnable, 
                                                                       queueManager.starvablePriorityQueueSet, 
                                                                       Clock.lastKnownForwardProgressingMillis() + 
                                                                         Integer.MAX_VALUE, 
                                                                       Integer.MAX_VALUE));
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
      if (shutdownStarted.getAndSet(true)) {
        return false; // shutdown already started
      } else {
        ShutdownRunnable sr = new ShutdownRunnable(this);
        queueManager.lowPriorityQueueSet
                    .addExecute(new ImmediateTaskWrapper(sr, queueManager.lowPriorityQueueSet.executeQueue));
        return true;
      }
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
      
      // submit task to wake up workers and start final consumption of tasks / thread shutdowns
      addPoolStateChangeTask(new InternalRunnable() {
        @Override
        public void run() {
          /* as long as we are continuing to run, we need to re-add ourself to ensure 
           * all threads are able to break out of task poll logic (once shutdown we stay shutdown)
           */
          addPoolStateChangeTask(this);
        }
      });
    }
    
    /**
     * When ever pool state is changed that is not checked in the pollTask loop (ie shutdown state, 
     * pool size), a task must be added that is continually added till the desired state is reached.
     * <p>
     * The purpose of this task is to break worker threads out of the tight loop for polling tasks, 
     * and instead get them to check the initial logic in {@link #workerIdle(Worker)} again.  This 
     * is in contrast to putting the logic in the poll task loop, which only incurs a performance 
     * penalty.
     */
    private void addPoolStateChangeTask(InternalRunnable task) {
      // add to starvable queue, since we only need these tasks to be consumed and ran when there 
      // is nothing else to run.
      queueManager.starvablePriorityQueueSet
                  .addExecute(new ImmediateTaskWrapper(task, 
                                                       queueManager.starvablePriorityQueueSet.executeQueue));
    }

    /**
     * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
     * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block until the 
     * timeout is reached.
     * 
     * @param timeoutMillis time to block and wait for thread pool to shutdown
     * @return {@code true} if the pool has shutdown, false if timeout was reached
     * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
     */
    public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
      long start = timeoutMillis < Long.MAX_VALUE ? 
        Clock.accurateForwardProgressingMillis() : Clock.lastKnownForwardProgressingMillis();
      synchronized (workerStopNotifyLock) {
        long remainingMillis;
        while ((! shutdownFinishing || currentPoolSize.get() > 0) && 
               (remainingMillis = timeoutMillis - (Clock.lastKnownForwardProgressingMillis() - start)) > 0) {
          workerStopNotifyLock.wait(remainingMillis);
        }
      }
      
      return shutdownFinishing && currentPoolSize.get() == 0;
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
     * <p>
     * If this was a reduction from the previous value, this call will examine idle workers to see 
     * if they should be expired.  If this call reduced the max pool size, and the current running 
     * thread count is higher than the new max size, this call will NOT block till the pool is 
     * reduced.  Instead as those workers complete, they will clean up on their own.
     * 
     * @param newPoolSize New core pool size, must be at least one
     */
    public void setPoolSize(int newPoolSize) {
      ArgumentVerifier.assertGreaterThanZero(newPoolSize, "newPoolSize");
      
      if (newPoolSize == maxPoolSize) {
        // short cut the lock
        return;
      }
      
      boolean poolSizeIncrease;
      synchronized (poolSizeChangeLock) {
        poolSizeIncrease = newPoolSize > this.maxPoolSize;
        
        this.maxPoolSize = newPoolSize;
      }
      
      handleMaxPoolSizeChange(poolSizeIncrease);
    }
    
    /**
     * Adjust the pools size by a given delta.  If the provided delta would result in a pool size 
     * of zero or less, then a {@link IllegalStateException} will be thrown.
     * 
     * @param delta Delta to adjust the max pool size by
     */
    public void adjustPoolSize(int delta) {
      if (delta == 0) {
        return;
      }
      
      synchronized (poolSizeChangeLock) {
        if (maxPoolSize + delta < 1) {
          throw new IllegalStateException(maxPoolSize + " " + delta + " must be at least 1");
        }
        this.maxPoolSize += delta;
      }
      
      handleMaxPoolSizeChange(delta > 0);
    }
    
    protected void handleMaxPoolSizeChange(boolean poolSizeIncrease) {
      if (poolSizeIncrease) {
        // now that pool size increased, start a worker so workers we can for the waiting tasks
        handleQueueUpdate();
      } else if (currentPoolSize.get() > maxPoolSize) {
        addPoolStateChangeTask(new InternalRunnable() {
          @Override
          public void run() {
            /* until the pool has reduced in size, we need to continue to add this task to 
             * wake threads out of the poll task loop
             */
            if (currentPoolSize.get() > maxPoolSize) {
              addPoolStateChangeTask(this);
            }
          }
        });
      }
    }

    /**
     * Check for the current quantity of threads running in this pool (either active or idle).
     * 
     * @return current thread count
     */
    public int getCurrentPoolSize() {
      return currentPoolSize.get();
    }

    /**
     * Call to check how many workers are currently executing tasks.
     * 
     * @return current number of workers executing tasks
     */
    public int getActiveTaskCount() {
      while (true) {
        int poolSize = currentPoolSize.get();
        int result = poolSize - idleWorkerCount.intValue();
        if (poolSize == currentPoolSize.get()) {
          return result;
        }
      }
    }

    /**
     * Ensures all threads have been started.  This will make new idle workers to accept tasks.
     */
    public void prestartAllThreads() {
      int casPoolSize;
      while ((casPoolSize = currentPoolSize.get()) < maxPoolSize) {
        if (currentPoolSize.compareAndSet(casPoolSize, casPoolSize + 1)) {
          makeNewWorker();
        }
      }
    }
    
    /**
     * This call creates and starts a new worker.  It does not modify {@link currentPoolSize} so 
     * that MUST be updated in a thread safe way before this is invoked.  As soon as the worker 
     * starts it will attempt to start taking tasks, no further action is needed.
     */
    protected void makeNewWorker() {
      Worker w = new Worker(this, threadFactory);
      w.start();
    }
    
    /**
     * Adds a worker to the head of the idle worker chain.
     * 
     * @param worker Worker that is ready to become idle
     */
    protected void addWorkerToIdleChain(Worker worker) {
      idleWorkerCount.increment();
      worker.waitingForUnpark = false;  // reset state before we park, avoid external interactions
      
      while (true) {
        Worker casWorker = idleWorker.get();
        // we can freely set this value until we get into the idle linked queue
        worker.nextIdleWorker = casWorker;
        if (idleWorker.compareAndSet(casWorker, worker)) {
          break;
        }
      }
    }
    
    /**
     * The counter part to {@link #addWorkerToIdleChain(Worker)}.  This function has no safety 
     * checks.  The worker provided MUST already be queued in the chain or problems will occur.
     * 
     * @param worker Worker reference to remove from the chain (can not be {@code null})
     */
    protected void removeWorkerFromIdleChain(Worker worker) {
      idleWorkerCount.decrement();
      
      // TODO - can we improve this by removing the lock
      
      /* We must lock here to avoid thread contention when removing from the chain.  This is 
       * the one place where we set the reference to a workers "nextIdleWorker" from a thread 
       * outside of the workers thread.  If we don't synchronize here, we may end up 
       * having workers disappear from the chain when the reference is nulled out.
       */
      synchronized (idleWorkerDequeLock) {
        Worker holdingWorker = idleWorker.get();
        if (holdingWorker == worker) {
          if (idleWorker.compareAndSet(worker, worker.nextIdleWorker)) {
            worker.nextIdleWorker = null;
            return;
          } else {
            /* because we can only queue in parallel, we know that the conflict was a newly queued 
             * worker.  In addition since we know that queued workers are added at the start, all 
             * that should be necessary is updating our holding worker reference
             */
            holdingWorker = idleWorker.get();
          }
        }
        
        // no need for null checks due to locking, we assume the worker is in the chain
        while (holdingWorker.nextIdleWorker != worker) {
          holdingWorker = holdingWorker.nextIdleWorker;
        }
        // now remove this worker from the chain
        holdingWorker.nextIdleWorker = worker.nextIdleWorker;
        // now out of the queue, lets clean up our reference
        worker.nextIdleWorker = null;
      }
    }

    /**
     * Invoked when a worker becomes idle.  This will provide another task for that worker, or 
     * block until a task is either ready, or the worker should be shutdown (either because pool 
     * was shut down, or max pool size changed).
     * 
     * @param worker Worker which is now idle and ready for a task
     * @return Task that is ready for immediate execution
     */
    public TaskWrapper workerIdle(Worker worker) {
      /* pool state checks, if any of these change we need a dummy task added to the queue to 
       * break out of the task polling loop below.  This is done as an optimization, to avoid 
       * needing to check these on every loop (since they rarely change)
       */
      int casPoolSize;
      while (true) {
        if (shutdownFinishing) {
          currentPoolSize.decrementAndGet();
          worker.stopIfRunning();
          return null;
        } else if ((casPoolSize = currentPoolSize.get()) > maxPoolSize) {
          if (currentPoolSize.compareAndSet(casPoolSize, casPoolSize - 1)) {
            worker.stopIfRunning();
            return null;
          } // else, retry, see if we need to shutdown
        } else {
          // pool state is consistent, we should keep running
          break;
        }
      }
      
      boolean queued = false;
      try {
        while (true) {
          TaskWrapper nextTask = 
              queueManager.getNextTask(stavableStartsThreads || casPoolSize == 1 || 
                                       currentPoolSize.get() >= maxPoolSize ||
                                       worker.nextIdleWorker != null ||
                                       shutdownStarted.get());

          if (nextTask == null) {
            if (queued) { // we can only park after we have queued, then checked again for a result
              Thread.interrupted(); // reset interrupted status before we block
              LockSupport.park();
              worker.waitingForUnpark = false;
              continue;
            } else {
              addWorkerToIdleChain(worker);
              queued = true;
            }
          } else {
            /* TODO - right now this has a a deficiency where a recurring period task can cut in 
             * the queue line.  The condition would be as follows:
             * 
             * * Thread 1 gets task to run...task is behind execution schedule, likely due to large queue
             * * Thread 2 gets same task
             * * Thread 1 gets reference, executes, task execution completes
             * * Thread 2 now gets the reference, and execution check and time check pass fine
             * * End result is that task has executed twice (on expected schedule), the second 
             *     execution was unfair since it was done without respects to queue order and 
             *     other tasks which are also likely behind execution schedule in this example
             *     
             * This should be very rare, but is possible.  The only way I see to solve this right 
             * now is to introduce locking.
             */
            // must get executeReference before time is checked
            short executeReference = nextTask.getExecuteReference();
            long taskDelay = nextTask.getScheduleDelay();
            if (taskDelay > 0) {
              if (taskDelay == Long.MAX_VALUE) {
                // the hack at construction/start is to avoid this from causing us to spin here 
                // if only one recurring task is scheduled (otherwise we would keep pulling 
                // that task while it's running)
                continue;
              }
              if (queued) {
                Thread.interrupted(); // reset interrupted status before we block
                if (nextTask.getPureRunTime() < workerTimedParkRunTime) {
                  // we can only park after we have queued, then checked again for a result
                  workerTimedParkRunTime = nextTask.getPureRunTime();
                  LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * taskDelay);
                  worker.waitingForUnpark = false;
                  workerTimedParkRunTime = Long.MAX_VALUE;
                  continue;
                } else {
                  // there is another worker already doing a timed park, so we can wait till woken up
                  LockSupport.park();
                  worker.waitingForUnpark = false;
                  continue;
                }
              } else {
                addWorkerToIdleChain(worker);
                queued = true;
              }
            } else if (nextTask.canExecute(executeReference)) {
              return nextTask;
            } else {
              // threading conflict when trying to consume tasks, back thread off with a yield
              // This helps with a potential tight loop resulting in issues with safe points
              Thread.yield();
            }
          }
        } // end pollTask loop
      } finally {
        // if queued, we must now remove ourselves, since worker is about to either shutdown or become active
        if (queued) {
          removeWorkerFromIdleChain(worker);
        }
        
        // wake up next worker so it can check if tasks are ready to consume
        handleQueueUpdate();
        
        Thread.interrupted();  // reset interrupted status if set
      }
    }

    @Override
    public void handleQueueUpdate() {
      while (true) {
        Worker nextIdleWorker = idleWorker.get();
        if (nextIdleWorker == null) {
          int casSize = currentPoolSize.get();
          if (casSize < maxPoolSize && ! shutdownFinishing) {
            if (currentPoolSize.compareAndSet(casSize, casSize + 1)) {
              // start a new worker for the next task
              makeNewWorker();
              break;
            } // else loop and retry logic
          } else {
            // pool has all threads started, or is shutting down
            break;
          }
        } else {
          if (! nextIdleWorker.waitingForUnpark) {
            nextIdleWorker.waitingForUnpark = true;
            LockSupport.unpark(nextIdleWorker.thread);
          }
          break;
        }
      }
    }
  }
  
  /**
   * Runnable which will run on pool threads.  It accepts runnables to run, and tracks usage.
   * 
   * @since 1.0.0
   */
  protected static class Worker extends AbstractService implements Runnable {
    protected final WorkerPool workerPool;
    protected final Thread thread;
    protected volatile Worker nextIdleWorker;
    protected volatile boolean waitingForUnpark;
    
    public Worker(WorkerPool workerPool, ThreadFactory threadFactory) {
      this.workerPool = workerPool;
      thread = threadFactory.newThread(this);
      if (thread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      nextIdleWorker = null;
      waitingForUnpark = false;
    }

    @Override
    protected void startupService() {
      thread.start();
    }

    @Override
    protected void shutdownService() {
      LockSupport.unpark(thread);
    }
    
    protected void executeTasksWhileRunning() {
      while (isRunning()) {
        TaskWrapper nextTask = workerPool.workerIdle(this);
        if (nextTask != null) {  // may be null if we are shutting down
          nextTask.runTask();
        }
      }
    }
    
    @Override
    public void run() {
      executeTasksWhileRunning();
      
      synchronized (workerPool.workerStopNotifyLock) {
        workerPool.workerStopNotifyLock.notifyAll();
      }
    }
  }
  
  /**
   * Runnable to be run after tasks already ready to execute.  That way this can be submitted with 
   * a {@link #execute(Runnable)} to ensure that the shutdown is fair for tasks that were already 
   * ready to be run/executed.  Once this runs the shutdown sequence will be finished, and no 
   * remaining asks in the queue can be executed.
   * 
   * @since 1.0.0
   */
  protected static final class ShutdownRunnable implements InternalRunnable {
    private final WorkerPool wm;
    
    protected ShutdownRunnable(WorkerPool wm) {
      this.wm = wm;
    }
    
    @Override
    public void run() {
      wm.finishShutdown();
    }
  }
}
