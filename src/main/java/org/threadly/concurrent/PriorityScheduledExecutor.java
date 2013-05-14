package org.threadly.concurrent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * Executor to run tasks, schedule tasks.  Unlike java.util.concurrent.ScheduledThreadPoolExecutor
 * this scheduled executor's pool size can grow and shrink based off usage.  It also has the benifit
 * that you can provide "low priority" tasks which will attempt to use existing workers and not instantly
 * create new threads on demand.  Thus allowing you to better take the benefits of a thread pool for tasks
 * which specific execution time is less important.
 * 
 * @author jent - Mike Jensen
 */
public class PriorityScheduledExecutor implements PrioritySchedulerInterface, 
                                                  LockFactory {
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT = 500;  // set to Long.MAX_VALUE to never create threads for low priority tasks
  protected static final TaskPriority GLOBAL_DEFAULT_PRIORITY = TaskPriority.High;
  protected static final boolean USE_DAEMON_THREADS = true;
  
  protected final TaskPriority defaultPriority;
  protected final VirtualLock highPriorityLock;
  protected final VirtualLock lowPriorityLock;
  protected final VirtualLock workersLock;
  protected final DynamicDelayQueue<TaskWrapper> highPriorityQueue;
  protected final DynamicDelayQueue<TaskWrapper> lowPriorityQueue;
  protected final Deque<Worker> availableWorkers;        // is locked around workersLock
  protected final ThreadFactory threadFactory;
  protected final TaskConsumer highPriorityConsumer;  // is locked around highPriorityLock
  protected final TaskConsumer lowPriorityConsumer;    // is locked around lowPriorityLock
  private volatile boolean running;
  private volatile int corePoolSize;
  private volatile int maxPoolSize;
  private volatile long keepAliveTimeInMs;
  private volatile long maxWaitForLowPriorityInMs;
  private volatile boolean allowCorePoolTimeout;
  private int currentPoolSize;  // is locked around workersLock

  /**
   * Constructs a new thread pool, though no threads will be started 
   * till it accepts it's first request.  This constructs a default 
   * priority of high (which makes sense for most use cases).  
   * It also defaults low priority worker wait as 500ms.
   * 
   * @param corePoolSize pool size that should be maintained
   * @param maxPoolSize maximum allowed thread count
   * @param keepAliveTimeInMs time to wait for a given thread to be idle before killing
   */
  public PriorityScheduledExecutor(int corePoolSize, int maxPoolSize,
                                   long keepAliveTimeInMs) {
    this(corePoolSize, maxPoolSize, keepAliveTimeInMs, 
         GLOBAL_DEFAULT_PRIORITY, DEFAULT_LOW_PRIORITY_MAX_WAIT);
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
    if (corePoolSize < 1) {
      throw new IllegalArgumentException("corePoolSize must be >= 1");
    } else if (maxPoolSize < corePoolSize) {
      throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
    } else if (keepAliveTimeInMs < 0) {
      throw new IllegalArgumentException("keepAliveTimeInMs must be >= 0");
    } else if (maxWaitForLowPriorityInMs < 0) {
      throw new IllegalArgumentException("maxWaitForLowPriorityInMs must be >= 0");
    }
    
    if (defaultPriority == null) {
      defaultPriority = GLOBAL_DEFAULT_PRIORITY;
    }
    
    this.defaultPriority = defaultPriority;
    highPriorityLock = makeLock();
    lowPriorityLock = makeLock();
    workersLock = makeLock();
    highPriorityQueue = new SynchronizedDynamicDelayQueue<TaskWrapper>(highPriorityLock);
    lowPriorityQueue = new SynchronizedDynamicDelayQueue<TaskWrapper>(lowPriorityLock);
    availableWorkers = new ArrayDeque<Worker>(maxPoolSize);
    threadFactory = new ThreadFactory() {
      private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = defaultFactory.newThread(runnable);
        
        thread.setDaemon(USE_DAEMON_THREADS);
        
        return thread;
      }
    };
    highPriorityConsumer = new TaskConsumer(highPriorityQueue, highPriorityLock, 
                                            new TaskAcceptor() {
      @Override
      public void acceptTask(TaskWrapper task) throws InterruptedException {
        runHighPriorityTask(task);
      }
    });
    lowPriorityConsumer = new TaskConsumer(lowPriorityQueue, lowPriorityLock, 
                                           new TaskAcceptor() {
      @Override
      public void acceptTask(TaskWrapper task) throws InterruptedException {
        runLowPriorityTask(task);
      }
    });
    running = true;
    this.corePoolSize = corePoolSize;
    this.maxPoolSize = maxPoolSize;
    this.keepAliveTimeInMs = keepAliveTimeInMs;
    this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
    this.allowCorePoolTimeout = false;
    currentPoolSize = 0;
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }
  
  /**
   * @return Set core pool size
   */
  public int getCorePoolSize() {
    return corePoolSize;
  }
  
  /**
   * @return Set max pool size
   */
  public int getMaxPoolSize() {
    return maxPoolSize;
  }
  
  /**
   * @return Set keep alive time
   */
  public long getKeepAliveTime() {
    return keepAliveTimeInMs;
  }
  
  /**
   * @return The current worker count
   */
  public int getCurrentPoolSize() {
    synchronized (workersLock) {
      return currentPoolSize;
    }
  }
  
  /**
   * Change the set core pool size.
   * 
   * @param corePoolSize New pool size.  Must be >= 1 and <= the set max pool size.
   */
  public void setCorePoolSize(int corePoolSize) {
    if (corePoolSize < 1) {
      throw new IllegalArgumentException("corePoolSize must be >= 1");
    } else if (maxPoolSize < corePoolSize) {
      throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
    }
    
    this.corePoolSize = corePoolSize;
  }
  
  /**
   * Change the set max pool size.
   * 
   * @param maxPoolSize New max pool size.  Must be >= 1 and >= the set core pool size.
   */
  public void setMaxPoolSize(int maxPoolSize) {
    if (maxPoolSize < 1) {
      throw new IllegalArgumentException("maxPoolSize must be >= 1");
    } else if (maxPoolSize < corePoolSize) {
      throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize");
    }
    
    this.maxPoolSize = maxPoolSize;
  }
  
  /**
   * Change the set idle thread keep alive time.
   * 
   * @param keepAliveTimeInMs New keep alive time in milliseconds.  Must be >= 0.
   */
  public void setKeepAliveTime(long keepAliveTimeInMs) {
    if (keepAliveTimeInMs < 0) {
      throw new IllegalArgumentException("keepAliveTimeInMs must be >= 0");
    }
    
    this.keepAliveTimeInMs = keepAliveTimeInMs;
  }
  
  /**
   * Changes the max wait time for an idle worker for low priority tasks.
   * Changing this will only take effect for future low priority tasks, it 
   * will have no impact for the current low priority task attempting to get 
   * a worker.
   * 
   * @param maxWaitForLowPriorityInMs New time for waiting for a thread in milliseconds.  Must be >= 0.
   */
  public void setMaxWaitForLowPriorityInMs(long maxWaitForLowPriorityInMs) {
    if (maxWaitForLowPriorityInMs < 0) {
      throw new IllegalArgumentException("maxWaitForLowPriorityInMs must be >= 0");
    }
    
    this.maxWaitForLowPriorityInMs = maxWaitForLowPriorityInMs;
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
        workersLock.signalAll();
      }
    }
  }

  /**
   * Changes the setting weather core threads are allowed to 
   * be killed if they remain idle.
   * 
   * @param value true if core threads should be expired when idle.
   */
  public void allowCoreThreadTimeOut(boolean value) {
    allowCorePoolTimeout = value;    
  }

  @Override
  public boolean isShutdown() {
    return ! running;
  }
  
  protected void clearTaskQueue() {
    synchronized (highPriorityLock) {
      synchronized (lowPriorityLock) {
        highPriorityConsumer.stop();
        lowPriorityConsumer.stop();
        
        synchronized (highPriorityQueue.getLock()) {
          Iterator<TaskWrapper> it = highPriorityQueue.iterator();
          while (it.hasNext()) {
            it.next().cancel();
          }
          lowPriorityQueue.clear();
        }
        synchronized (lowPriorityQueue.getLock()) {
          Iterator<TaskWrapper> it = lowPriorityQueue.iterator();
          while (it.hasNext()) {
            it.next().cancel();
          }
          lowPriorityQueue.clear();
        }
      }
    }
  }
  
  protected void shutdownAllWorkers() {
    synchronized (workersLock) {
      Iterator<Worker> it = availableWorkers.iterator();
      while (it.hasNext()) {
        it.next().stop();
        it.remove();
      }
    }
  }

  /**
   * Stops any tasks from continuing to run and destroys all worker threads.
   */
  public void shutdown() {
    running = false;
    clearTaskQueue();
    shutdownAllWorkers();
  }
  
  protected void verifyNotShutdown() {
    if (! running) {
      throw new IllegalStateException("Thread pool shutdown");
    }
  }
  
  protected static boolean removeFromTaskQueue(DynamicDelayQueue<TaskWrapper> queue, 
                                               Runnable task) {
    synchronized (queue.getLock()) {
      Iterator<TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        if (tw.task.equals(task)) {
          tw.cancel();
          it.remove();
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Removes the task from the execution queue.  It is possible
   * for the task to still run until this call has returned.
   * 
   * @param task The original task provided to the executor
   * @return true if the task was found and removed
   */
  public boolean remove(Runnable task) {
    return removeFromTaskQueue(highPriorityQueue, task) || 
             removeFromTaskQueue(lowPriorityQueue, task);
  }

  /**
   * Executes a task as soon as possible with the default priority
   * 
   * @param task Task to execute
   */
  @Override
  public void execute(Runnable task) {
    execute(task, defaultPriority);
  }

  /**
   * Executes the task as soon as possible with the given priority.
   * 
   * @param task Task to execute
   * @param priority Priority for task
   */
  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
  }

  /**
   * Schedule a task with a given delay and a default priority.
   * 
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, defaultPriority);
  }

  /**
   * Schedule a task with a given delay and a specified priority.
   * 
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   * @param priority Priority to give task for execution
   */
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

  /**
   * Schedule a recurring task to run with the default priority.
   * 
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   */
  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                           defaultPriority);
  }

  /**
   * Schedule a recurring task to run with a provided priority.
   * 
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   * @param priority Priority for task to run at
   */
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
    switch (task.priority) {
      case High:
        verifyNotShutdown();
        ClockWrapper.stopForcingUpdate();
        try {
          ClockWrapper.updateClock();
          highPriorityQueue.add(task);
        } finally {
          ClockWrapper.resumeForcingUpdate();
        }
        highPriorityConsumer.maybeStart();
        break;
      case Low:
        verifyNotShutdown();
        ClockWrapper.stopForcingUpdate();
        try {
          ClockWrapper.updateClock();
          lowPriorityQueue.add(task);
        } finally {
          ClockWrapper.resumeForcingUpdate();
        }
        lowPriorityConsumer.maybeStart();
        break;
      default:
        throw new UnsupportedOperationException("Priority not implemented: " + task.priority);
    }
  }
  
  protected Worker getExistingWorker(long maxWaitForLowPriorityInMs) throws InterruptedException {
    synchronized (workersLock) {
      long startTime = ClockWrapper.getAccurateTime();
      long waitTime = maxWaitForLowPriorityInMs;
      while (availableWorkers.isEmpty() && waitTime > 0) {
        if (waitTime == Long.MAX_VALUE) {  // prevent overflow
          workersLock.await();
        } else {
          long elapsedTime = ClockWrapper.getAccurateTime() - startTime;
          waitTime = maxWaitForLowPriorityInMs - elapsedTime;
          workersLock.await(waitTime);
        }
      }
      
      if (availableWorkers.isEmpty()) {
        return null;  // we exceeded the wait time
      } else {
        // always remove from the front, to get the newest worker
        return availableWorkers.removeFirst();
      }
    }
  }
  
  protected Worker makeNewWorker() {
    synchronized (workersLock) {
      Worker w = new Worker();
      currentPoolSize++;
      w.start();
  
      // will be added to available workers when done with first task
      return w;
    }
  }
  
  protected void runHighPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (running) {
        if (currentPoolSize >= maxPoolSize) {
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
      w.nextTask(task);
    }
  }
  
  protected void runLowPriorityTask(TaskWrapper task) throws InterruptedException {
    Worker w = null;
    synchronized (workersLock) {
      if (running) {
        long waitTime;
        if (currentPoolSize >= maxPoolSize) {
          waitTime = Long.MAX_VALUE;
        } else {
          waitTime = maxWaitForLowPriorityInMs;
        }
        w = getExistingWorker(waitTime);
        if (w == null) {
          // this means we expired past our wait time, so just make a new worker
          if (currentPoolSize >= maxPoolSize) {
            // more workers were created while waiting, now have exceeded our max
            w = getExistingWorker(Long.MAX_VALUE);
          } else {
            w = makeNewWorker();
          }
        }
      }
    }
    
    if (w != null) {  // may be null if shutdown
      w.nextTask(task);
    }
  }
  
  protected void lookForExpiredWorkers() {
    synchronized (workersLock) {
      long now = ClockWrapper.getLastKnownTime();
      // we search backwards because the oldest workers will be at the back of the stack
      while ((currentPoolSize > corePoolSize || allowCorePoolTimeout) && 
             ! availableWorkers.isEmpty() && 
             now - availableWorkers.getLast().getLastRunTime() > keepAliveTimeInMs) {
        Worker w = availableWorkers.removeLast();
        killWorker(w);
      }
    }
  }
  
  private void killWorker(Worker w) {
    synchronized (workersLock) {
      w.stop();
      currentPoolSize--;
    }
  }
  
  protected void workerDone(Worker worker) {
    synchronized (workersLock) {
      if (running) {
        // always add to the front so older workers are at the back
        availableWorkers.addFirst(worker);
      
        lookForExpiredWorkers();
            
        workersLock.signalAll();
      } else {
        killWorker(worker);
      }
    }
  }

  @Override
  public VirtualLock makeLock() {
    return new NativeLock();
  }
  
  protected class TaskConsumer implements Runnable {
    private final DynamicDelayQueue<TaskWrapper> workQueue;
    private final VirtualLock queueLock;
    private final TaskAcceptor acceptor;
    private volatile boolean started;
    private volatile boolean stopped;
    private volatile Thread runningThread;
    
    protected TaskConsumer(DynamicDelayQueue<TaskWrapper> workQueue, 
                           VirtualLock queueLock, TaskAcceptor acceptor) {
      this.workQueue = workQueue;
      this.queueLock = queueLock;
      this.acceptor = acceptor;
      started = false;
      stopped = false;
      runningThread = null;
    }

    public boolean isRunning() {
      return started && ! stopped;
    }
    
    public void maybeStart() {
      // this looks like a double check but due to being volatile and only changing one direction should be safe
      if (started) {
        return;
      }
      
      synchronized (queueLock) {
        if (started) {
          return;
        }

        started = true;
        runningThread = new Thread(this);
        runningThread.setDaemon(USE_DAEMON_THREADS);
        runningThread.setName("ScheduledExecutor task consumer thread");
        runningThread.start();
      }
    }
    
    public void stop() {
      // this looks like a double check but due to being volatile and only changing one direction should be safe
      if (stopped || ! started) {
        return;
      }
      
      synchronized (queueLock) {
        if (stopped || ! started) {
          return;
        }

        stopped = true;
        Thread runningThread = this.runningThread;
        this.runningThread = null;
        runningThread.interrupt();
      }
    }
    
    @Override
    public void run() {
      while (! stopped) {
        try {
          TaskWrapper task;
          synchronized (queueLock) {  // must lock as same lock for removal to ensure that task can be found for removal
            task = workQueue.take();
            task.executing();  // for recurring tasks this will put them back into the queue
          }
          try {
            acceptor.acceptTask(task);
          } catch (InterruptedException e) {
            stop();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
        }
      }
    }
  }
  
  protected interface TaskAcceptor {
    public void acceptTask(TaskWrapper task) throws InterruptedException;
  }
  
  protected class Worker implements Runnable {
    private final VirtualLock taskNotifyLock;
    private final Thread thread;
    private volatile long lastRunTime;
    private boolean running;
    private volatile TaskWrapper nextTask;
    
    protected Worker() {
      this.taskNotifyLock = makeLock();
      thread = threadFactory.newThread(this);
      running = true;
      lastRunTime = ClockWrapper.getLastKnownTime();
      nextTask = null;
    }
    
    public void stop() {
      synchronized (taskNotifyLock) {
        running = false;
        
        taskNotifyLock.signalAll();
      }
    }

    public void start() {
      if (thread.isAlive()) {
        return;
      } else {
        thread.start();
      }
    }
    
    public void nextTask(TaskWrapper task) {
      synchronized (taskNotifyLock) {
        if (! running) {
          throw new IllegalStateException("Worker has been killed");
        } else if (nextTask != null) {
          throw new IllegalStateException("Already has a task");
        }
        
        nextTask = task;
        taskNotifyLock.signalAll();
      }
    }
    
    public void blockTillNextTask() throws InterruptedException {
      if (nextTask != null) {
        return;
      }
      
      synchronized (taskNotifyLock) {
        while (nextTask == null && running) {
          taskNotifyLock.await();
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
          if (t instanceof InterruptedException || 
              t instanceof OutOfMemoryError) {
            killWorker(this);  // this will stop the worker, and thus prevent it from calling workerDone
          }
        } finally {
          nextTask = null;
          if (running) {
            lastRunTime = ClockWrapper.getLastKnownTime();
            workerDone(this);
          }
        }
      }
    }
    
    public long getLastRunTime() {
      return lastRunTime;
    }
  }
  
  private enum TaskType {OneTime, Recurring};
  
  protected abstract class TaskWrapper implements Delayed, Runnable {
    public final TaskType taskType;
    public final TaskPriority priority;
    protected final Runnable task;
    protected volatile boolean canceled;
    
    protected TaskWrapper(TaskType taskType, 
                          Runnable task, 
                          TaskPriority priority) {
      this.taskType = taskType;
      this.priority = priority;
      this.task = task;
      canceled = false;
    }
    
    public void cancel() {
      canceled = true;
    }
    
    public abstract void executing();

    @Override
    public int compareTo(Delayed o) {
      if (this == o) {
        return 0;
      } else {
        long thisDelay = this.getDelay(TimeUnit.MILLISECONDS);
        long otherDelay = o.getDelay(TimeUnit.MILLISECONDS);
        if (thisDelay == otherDelay) {
          return 0;
        } else if (thisDelay > otherDelay) {
          return 1;
        } else {
          return -1;
        }
      }
    }
    
    @Override
    public String toString() {
      return task.toString();
    }
  }
  
  protected class OneTimeTaskWrapper extends TaskWrapper {
    private final long runTime;
    
    protected OneTimeTaskWrapper(Runnable task, TaskPriority priority, long delay) {
      super(TaskType.OneTime, task, priority);
      
      runTime = ClockWrapper.getAccurateTime() + delay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return TimeUnit.MILLISECONDS.convert(runTime - ClockWrapper.getAccurateTime(), unit);
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
  
  protected class RecurringTaskWrapper extends TaskWrapper {
    private final long recurringDelay;
    //private volatile long maxExpectedRuntime;
    private volatile boolean executing;
    private long nextRunTime;
    
    protected RecurringTaskWrapper(Runnable task, TaskPriority priority, 
                                   long initialDelay, long recurringDelay) {
      super(TaskType.Recurring, task, priority);
      
      this.recurringDelay = recurringDelay;
      //maxExpectedRuntime = -1;
      executing = false;
      this.nextRunTime = ClockWrapper.getAccurateTime() + initialDelay;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      if (executing) {
        return Long.MAX_VALUE;
      } else {
        return TimeUnit.MILLISECONDS.convert(nextRunTime - ClockWrapper.getAccurateTime(), unit);
      }
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
      nextRunTime = ClockWrapper.getAccurateTime() + recurringDelay;
      executing = false;
      
      // now that nextRunTime has been set, resort the queue
      switch (priority) {
        case High:
          synchronized (highPriorityLock) {
            if (running) {
              ClockWrapper.stopForcingUpdate();
              try {
                ClockWrapper.updateClock();
                highPriorityQueue.reposition(this);
              } finally {
                ClockWrapper.resumeForcingUpdate();
              }
            }
          }
          break;
        case Low:
          synchronized (lowPriorityLock) {
            if (running) {
              ClockWrapper.stopForcingUpdate();
              try {
                ClockWrapper.updateClock();
                lowPriorityQueue.reposition(this);
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
          reschedule();
        }
      }
    }
  }
}