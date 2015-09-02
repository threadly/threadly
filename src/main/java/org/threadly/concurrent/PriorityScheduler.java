package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

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
public class PriorityScheduler extends AbstractPriorityScheduler {
  protected static final boolean DEFAULT_NEW_THREADS_DAEMON = true;
  
  protected final WorkerPool workerPool;
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
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
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
    super(defaultPriority);
    
    this.workerPool = workerPool;
    taskConsumer = new QueueManager(workerPool, 
                                    "task consumer for " + this.getClass().getSimpleName(), 
                                    maxWaitForLowPriorityInMs);
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
  @Override
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
  
  @Override
  public void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs) {
    taskConsumer.setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
  }
  
  @Override
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
      queueSet.addExecute(new OneTimeTaskWrapper(sr, queueSet.executeQueue, 
                                                 Clock.lastKnownForwardProgressingMillis()));
    }
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
    List<Runnable> awaitingTasks = taskConsumer.stopAndClearQueue();
    workerPool.finishShutdown();
    
    return awaitingTasks;
  }
  
  /**
   * Makes a new {@link PrioritySchedulerLimiter} that uses this pool as it's execution source.
   * 
   * @param maxConcurrency maximum number of threads to run in parallel in sub pool
   * @return newly created {@link PrioritySchedulerLimiter} that uses this pool as it's execution source
   */
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
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
   * For non-recurring tasks using a future and calling 
   * {@link java.util.concurrent.Future#cancel(boolean)} can be a better solution.
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
   * For non-recurring tasks using a future and calling 
   * {@link java.util.concurrent.Future#cancel(boolean)} can be a better solution.
   * 
   * @param task The original callable provided to the executor
   * @return {@code true} if the callable was found and removed
   */
  @Override
  public boolean remove(Callable<?> task) {
    return taskConsumer.remove(task);
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    QueueSet queueSet = taskConsumer.getQueueSet(priority);
    OneTimeTaskWrapper result;
    if (delayInMillis == 0) {
      addToExecuteQueue(queueSet, 
                        (result = new OneTimeTaskWrapper(task, queueSet.executeQueue, 
                                                         Clock.lastKnownForwardProgressingMillis())));
    } else {
      addToScheduleQueue(queueSet, 
                         (result = new OneTimeTaskWrapper(task, queueSet.scheduleQueue, 
                                                          Clock.accurateForwardProgressingMillis() + delayInMillis)));
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

    QueueSet queueSet = taskConsumer.getQueueSet(priority);
    addToScheduleQueue(queueSet, 
                       new RecurringDelayTaskWrapper(task, queueSet, 
                                                     Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                     recurringDelay));
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
    addToScheduleQueue(queueSet, 
                       new RecurringRateTaskWrapper(task, queueSet, 
                                                    Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                    period));
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

  @Override
  protected QueueSet getQueueSet(TaskPriority priority) {
    return taskConsumer.getQueueSet(priority);
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
  protected static class QueueManager extends AbstractService 
                                      implements QueueSetListener, Runnable {
    protected final WorkerPool workerPool;
    protected final QueueSet highPriorityQueueSet;
    protected final QueueSet lowPriorityQueueSet;
    protected final QueueSet starvablePriorityQueueSet;
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
      this.highPriorityQueueSet = new QueueSet(this);
      this.lowPriorityQueueSet = new QueueSet(this);
      this.starvablePriorityQueueSet = new QueueSet(this);
      
      // call to verify and set values
      setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
      
      start();
    }

    @Override
    public void handleQueueUpdate() {
      LockSupport.unpark(runningThread);
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
      } else if (priority == TaskPriority.Low) {
        return lowPriorityQueueSet;
      } else {
        return starvablePriorityQueueSet;
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

      ArrayList<TaskWrapper> wrapperList = new ArrayList<TaskWrapper>(getScheduledTaskCount());
      highPriorityQueueSet.drainQueueInto(wrapperList);
      lowPriorityQueueSet.drainQueueInto(wrapperList);
      starvablePriorityQueueSet.drainQueueInto(wrapperList);
      wrapperList.trimToSize();
      
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
        TaskWrapper nextTask = getNextTask(highPriorityQueueSet, lowPriorityQueueSet, 
                                           maxWaitForLowPriorityInMs);
        if (nextTask == null) {
          TaskWrapper nextStarvableTask = starvablePriorityQueueSet.getNextTask();
          if (nextStarvableTask == null) {
            LockSupport.park();
          } else {
            long nextStarvableTaskDelay = nextStarvableTask.getScheduleDelay();
            if (nextStarvableTaskDelay > 0) {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextStarvableTaskDelay);
            } else if (nextStarvableTask.canExecute()) {
              return nextStarvableTask;
            }
          }
        } else {
          long nextTaskDelay = nextTask.getScheduleDelay();
          if (nextTaskDelay > 0) {
            TaskWrapper nextStarvableTask = starvablePriorityQueueSet.getNextTask();
            if (nextStarvableTask == null) {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextTaskDelay);
            } else {
              long nextStarvableTaskDelay = nextStarvableTask.getScheduleDelay();
              if (nextStarvableTaskDelay > 0) {
                LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * 
                                        (nextTaskDelay > nextStarvableTaskDelay ? 
                                           nextStarvableTaskDelay : nextTaskDelay));
              } else if (nextStarvableTask.canExecute()) {
                return nextStarvableTask;
              }
            }
          } else if (nextTask.canExecute()) {
            return nextTask;
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
      return highPriorityQueueSet.queueSize() + lowPriorityQueueSet.queueSize() + 
               starvablePriorityQueueSet.queueSize();
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
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task) || 
               starvablePriorityQueueSet.remove(task);
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
      return highPriorityQueueSet.remove(task) || lowPriorityQueueSet.remove(task) || 
               starvablePriorityQueueSet.remove(task);
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
   * <p>Runnable to be run after tasks already ready to execute.  That way this can be submitted 
   * with a {@link #execute(Runnable)} to ensure that the shutdown is fair for tasks that were 
   * already ready to be run/executed.  Once this runs the shutdown sequence will be finished, and 
   * no remaining asks in the queue can be executed.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected static class ShutdownRunnable implements InternalRunnable {
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
