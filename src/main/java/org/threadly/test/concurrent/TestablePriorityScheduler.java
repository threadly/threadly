package org.threadly.test.concurrent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.VirtualCallable;
import org.threadly.concurrent.VirtualRunnable;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.StaticCancellationException;
import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.lock.TestableLock;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.ListUtils;

/**
 * Scheduler which is designed to be used during unit testing.
 * Although it actually runs multiple threads, it only has one 
 * thread actively executing at a time (thus simulating single threaded).
 * 
 * The scheduler uses .awaits() and .sleep()'s to {@link TestableLock}'s as opportunities
 * to simulate multiple threads.  When you call .tick() you progress forward
 * externally scheduled threads, or possibly threads which are sleeping.
 * 
 * @author jent - Mike Jensen
 */
public class TestablePriorityScheduler implements PrioritySchedulerInterface, 
                                                  TestableExecutor, 
                                                  LockFactory {
  protected final Executor executor;
  protected final TaskPriority defaultPriority;
  protected final LinkedList<RunnableContainer> taskQueue;
  private final Map<TestableLock, NotifyObject> waitingThreads;
  private final Object queueLock;
  private final Object tickLock;
  private final BlockingQueue<Object> threadQueue;
  private volatile int waitingForThreadCount;
  private volatile Object runningLock;
  private long nowInMillis;

  /**
   * Constructs a new {@link TestablePriorityScheduler} with the backed thread pool.
   * Because this only simulates threads running in a single threaded way, 
   * it must have a sufficiently large thread pool to back that.
   * 
   * @param scheduler Scheduler which will be used to execute new threads are necessary
   */
  public TestablePriorityScheduler(PriorityScheduledExecutor scheduler) {
    this(scheduler, scheduler.getDefaultPriority());
  }

  /**
   * Constructs a new {@link TestablePriorityScheduler} with the backed thread pool.
   * Because this only simulates threads running in a single threaded way, 
   * it must have a sufficiently large thread pool to back that.
   * 
   * @param executor Executor which will be used to execute new threads are necessary
   * @param defaultPriority Default priority for tasks where it is not provided
   */
  public TestablePriorityScheduler(Executor executor, TaskPriority defaultPriority) {
    if (executor == null) {
      throw new IllegalArgumentException("Must provide backing scheduler");
    }
    if (defaultPriority == null) {
      defaultPriority = TaskPriority.High;
    }
    
    this.executor = executor;
    this.defaultPriority = defaultPriority;
    taskQueue = new LinkedList<RunnableContainer>();
    waitingThreads = new HashMap<TestableLock, NotifyObject>();
    queueLock = new Object();
    tickLock = new Object();
    threadQueue = new ArrayBlockingQueue<Object>(1, true);
    threadQueue.offer(new Object());
    waitingForThreadCount = 0;
    runningLock = null;
    nowInMillis = Clock.accurateTime();
  }
  
  /**
   * Getter for the executor that was provided at construction time.
   * 
   * @return executor that was provided at construction time
   */
  public Executor getExecutor() {
    return executor;
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
    return submit(task, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return submit(task, result, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, 
                                        TaskPriority priority) {
    return submitScheduled(task, result, 0, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return submit(task, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, TaskPriority priority) {
    return submitScheduled(task, 0, priority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs) {
    return submitScheduled(task, result, delayInMs, defaultPriority);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs,
                                             TaskPriority priority) {
    return submitScheduled(task, null, delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, TaskPriority priority) {
    OneTimeFutureRunnable<T> otfr = new OneTimeFutureRunnable<T>(task, result, 
                                                                 delayInMs, priority, 
                                                                 new NativeLock());
    add(otfr);
    
    return otfr;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return submitScheduled(task, delayInMs, defaultPriority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs,
                                                 TaskPriority priority) {
    OneTimeFutureRunnable<T> otfr = new OneTimeFutureRunnable<T>(task, delayInMs, priority, 
                                                                 new NativeLock());
    add(otfr);
    
    return otfr;
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, defaultPriority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    add(new OneTimeRunnable(task, delayInMs, priority));
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                           defaultPriority);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    add(new RecurringRunnable(task, initialDelay, 
                              recurringDelay, priority));
  }
  
  protected void add(RunnableContainer runnable) {
    synchronized (queueLock) {
      int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, runnable);
      
      taskQueue.add(insertionIndex, runnable);
    }
  }

  @Override
  public boolean isShutdown() {
    return false;
  }
  
  private void wantToRun(boolean mainThread, Runnable runBeforeBlocking) {
    synchronized (tickLock) {
      waitingForThreadCount++;
      if (runBeforeBlocking != null) {
        runBeforeBlocking.run();
      }
      
      while (mainThread && waitingForThreadCount > 1) {
        // give others a chance
        try {
          tickLock.wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    try {
      Object runningLock = threadQueue.take();
      if (this.runningLock != null) {
        throw new IllegalStateException("Running lock already set: " + this.runningLock);
      } else {
        this.runningLock = runningLock;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
  
  private void yielding() {
    if (runningLock == null) {
      throw new IllegalStateException("No running lock to provide");
    }
    synchronized (tickLock) {
      waitingForThreadCount--;
      
      tickLock.notify();
    }
    
    Object runningLock = this.runningLock;
    this.runningLock = null;
    threadQueue.offer(runningLock);
  }
  
  /**
   * This ticks forward one step based off the current time of calling.  
   * It runs as many events are ready for the time provided, and will block
   * until all those events have completed (or are waiting or sleeping). 
   * 
   * @return qty of steps taken forward.  Returns zero if no events to run.
   */
  public int tick() {
    return tick(Clock.accurateTime());
  }
  
  protected void updateTime(long currentTime) {
    if (currentTime < nowInMillis) {
      throw new IllegalArgumentException("Can not go backwards in time");
    }
    
    nowInMillis = currentTime;
  }
  
  /**
   * This ticks forward one step based off the current time of calling.  
   * It runs as many events are ready for the time provided, and will block
   * until all those events have completed (or are waiting or sleeping). 
   * 
   * @param currentTime time reference for the scheduler to use to look for waiting events 
   * @return qty of steps taken forward.  Returns zero if no events to run for provided time.
   */
  public int tick(long currentTime) {
    updateTime(currentTime);
    
    wantToRun(true, null);
    
    int ranTasks = 0;
    RunnableContainer nextTask = getNextTask();
    while (nextTask != null) {
      ranTasks++;
      try {
        handleTask(nextTask);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      nextTask = getNextTask();
    }
    
    // we must yield right before we return so next tick can run
    yielding();
    
    if (threadQueue.size() != 1) {
      throw new IllegalStateException("Someone took the lock before we returned: " + 
                                        threadQueue.size() + " - " + waitingForThreadCount);
    } else if (waitingForThreadCount != 0) {
      throw new IllegalStateException("Still threads waiting to run: " + waitingForThreadCount);
    }
    
    return ranTasks;
  }
  
  protected RunnableContainer getNextTask() {
    synchronized (queueLock) {
      RunnableContainer firstResult = null;
      
      long nextDelay = Long.MIN_VALUE;
      Iterator<RunnableContainer> it = taskQueue.iterator();
      while (it.hasNext() && nextDelay <= 0) {
        RunnableContainer next = it.next();
        nextDelay = next.getDelay(TimeUnit.MILLISECONDS);
        if (nextDelay <= 0) {
          if (firstResult == null) {
            if (next.priority == TaskPriority.Low) {
              firstResult = next;
            } else {
              it.remove();
              return next;
            }
          } else if (next.priority == TaskPriority.High) {
            it.remove();
            return next;
          }
        }
      }
      
      if (firstResult != null) {
        taskQueue.removeFirst();
      }
      return firstResult;
    }
  }
  
  private void handleTask(RunnableContainer nextTask) throws InterruptedException {
    nextTask.prepareForExcute();
    executor.execute(nextTask);
    
    yielding(); // yield to new task
    nextTask.blockTillStarted();
    
    wantToRun(true, null);  // wait till we can run again
  }

  @Override
  public VirtualLock makeLock() {
    return new TestableLock(this);
  }

  @Override
  public boolean isNativeLockFactory() {
    return false;
  }

  @Override
  public void handleWaiting(TestableLock lock) throws InterruptedException {
    NotifyObject no = waitingThreads.get(lock);
    if (no == null) {
      no = new NotifyObject(lock);
      waitingThreads.put(lock, no);
    }
    
    no.yield();
  }

  @Override
  public void handleWaiting(final TestableLock lock, 
                            long waitTimeInMs) throws InterruptedException {
    final boolean[] notified = new boolean[] { false };
    // schedule runnable to wake up in case not signaled
    schedule(new Runnable() {
      @Override
      public void run() {
        if (! notified[0]) {
          handleSignal(lock);
        }
      }
    }, waitTimeInMs);
    
    handleWaiting(lock);
    notified[0] = true;
  }

  @Override
  public void handleSignal(TestableLock lock) {
    NotifyObject no = waitingThreads.get(lock);
    if (no != null) {
      // schedule task to wake up other thread
      add(new WakeUpThread(no, false, 0));
    }
  }

  @Override
  public void handleSignalAll(TestableLock lock) {
    NotifyObject no = waitingThreads.get(lock);
    if (no != null) {
      // schedule task to wake up other thread
      add(new WakeUpThread(no, true, 0));
    }
  }

  @Override
  public void handleSleep(long sleepTime) throws InterruptedException {
    NotifyObject sleepLock = new NotifyObject(new Object());
    add(new WakeUpThread(sleepLock, false, sleepTime));
    
    sleepLock.yield();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }
  
  /**
   * Container to hold some general logic that must happen
   * for anything submitted to the thread pool.
   * 
   * @author jent - Mike Jensen
   */
  protected abstract class RunnableContainer implements Runnable, Delayed {
    protected final Runnable runnable;
    protected final TaskPriority priority;
    private volatile boolean running;
    
    protected RunnableContainer(Runnable runnable, TaskPriority priority) {
      this.runnable = runnable;
      this.priority = priority;
      running = false;
    }
    
    public void blockTillStarted() {
      while (! running) {
        // spin
      }
    }

    public void prepareForExcute() {
      running = false;
    }

    @Override
    public void run() {
      wantToRun(false, null);  // must become running thread
      running = true;
      try {
        run(TestablePriorityScheduler.this);
      } finally {
        handleDone();
      }
    }
    
    protected void handleDone() {
      yielding();
    }
    
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
    
    public abstract void run(LockFactory scheduler);
  }
  
  /**
   * Container for runnables which are only run once.
   * 
   * @author jent - Mike Jensen
   */
  protected class OneTimeRunnable extends RunnableContainer {
    private final long runTime;
    
    protected OneTimeRunnable(Runnable runnable, long delay, 
                              TaskPriority priority) {
      super(runnable, priority);
      
      this.runTime = nowInMillis + delay;
    }
    
    @Override
    public void run(LockFactory scheduler) {
      if (runnable instanceof VirtualRunnable) {
        ((VirtualRunnable)runnable).run(scheduler);
      } else {
        runnable.run();
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(runTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * Container for runnables which are only run once.  And also need 
   * to implement the {@link ListenableFuture} interface.
   * 
   * @author jent - Mike Jensen
   */
  protected class OneTimeFutureRunnable<T> extends OneTimeRunnable
                                           implements ListenableFuture<T> {
    protected final Map<Runnable, Executor> listeners;
    private final Callable<T> callable;
    private final VirtualLock lock;
    private final T runnableResult;
    private boolean canceled;
    private boolean started;
    private boolean done;
    private Exception failure;
    private T result;

    protected OneTimeFutureRunnable(Runnable runnable, T runnableResult, long delay, 
                                    TaskPriority priority, VirtualLock lock) {
      super(runnable, delay, priority);
      
      listeners = new HashMap<Runnable, Executor>();
      callable = null;
      this.lock = lock;
      this.runnableResult = runnableResult;
      canceled = false;
      started = false;
      done = false;
      failure = null;
      result = null;
    }

    protected OneTimeFutureRunnable(Callable<T> callable, long delay, 
                                    TaskPriority priority, VirtualLock lock) {
      super(null, delay, priority);
      
      listeners = new HashMap<Runnable, Executor>();
      this.callable = callable;
      this.lock = lock;
      this.runnableResult = null;
      canceled = false;
      started = false;
      done = false;
      failure = null;
      result = null;
    }
    
    @Override
    public void run(LockFactory scheduler) {
      try {
        boolean shouldRun = false;
        synchronized (lock) {
          if (! canceled) {
            started = true;
            shouldRun = true;
          }
        }
        
        if (shouldRun) {
          if (runnable != null) {
            if (runnable instanceof VirtualRunnable) {
              ((VirtualRunnable)runnable).run(scheduler);
            } else {
              runnable.run();
            }
            result = runnableResult;
          } else {
            if (callable instanceof VirtualCallable) {
              result = ((VirtualCallable<T>)callable).call(scheduler);
            } else {
              result = callable.call();
            }
          }
        }
        
        synchronized (lock) {
          done = true;
          
          callListeners();
          
          lock.signalAll();
        }
      } catch (Exception e) {
        synchronized (lock) {
          done = true;
          failure = e;
          
          callListeners();
          
          lock.signalAll();
        }
        
        throw ExceptionUtils.makeRuntime(e);
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (lock) {
        canceled = true;
        
        callListeners();
        
        lock.signalAll();
      
        return ! started;
      }
    }

    @Override
    public boolean isDone() {
      synchronized (lock) {
        return done;
      }
    }

    @Override
    public boolean isCancelled() {
      synchronized (lock) {
        return canceled && ! started;
      }
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      try {
        return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // basically impossible
        throw ExceptionUtils.makeRuntime(e);
      }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      long startTime = Clock.accurateTime();
      long timeoutInMs = TimeUnit.MILLISECONDS.convert(timeout, unit);
      synchronized (lock) {
        long waitTime = timeoutInMs - (Clock.accurateTime() - startTime);
        while (! done && waitTime > 0) {
          lock.await(waitTime);
          waitTime = timeoutInMs - (Clock.accurateTime() - startTime);
        }
        if (canceled || 
            (failure != null && 
              (failure == StaticCancellationException.instance() || failure instanceof CancellationException))) {
          throw StaticCancellationException.instance();
        } else if (failure != null) {
          throw new ExecutionException(failure);
        } else if (! done) {
          throw new TimeoutException();
        }
        
        return result;
      }
    }
    
    private void callListeners() {
      synchronized (this) {
        Iterator<Entry<Runnable, Executor>> it = listeners.entrySet().iterator();
        while (it.hasNext()) {
          Entry<Runnable, Executor> listener = it.next();
          runListener(listener.getKey(), listener.getValue(), false);
        }
        
        listeners.clear();
      }
    }
    
    private void runListener(Runnable listener, Executor executor, 
                             boolean throwException) {
      if (executor == null) {
        executor = TestablePriorityScheduler.this;
      }
      
      executor.execute(listener);
    }

    @Override
    public void addListener(Runnable listener) {
      addListener(listener, null);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      synchronized (this) {
        if (done || canceled) {
          runListener(listener, executor, true);
        } else {
          listeners.put(listener, executor);
        }
      }
    }
  }
  
  /**
   * Container for runnables which will run multiple times.
   * 
   * @author jent - Mike Jensen
   */
  protected class RecurringRunnable extends RunnableContainer {
    private final long recurringDelay;
    private long nextRunTime;
    
    protected RecurringRunnable(Runnable runnable, 
                                long initialDelay, 
                                long recurringDelay, 
                                TaskPriority priority) {
      super(runnable, priority);
      
      this.recurringDelay = recurringDelay;
      nextRunTime = nowInMillis + initialDelay;
    }
    
    @Override
    public void run(LockFactory scheduler) {
      try {
        if (runnable instanceof VirtualRunnable) {
          ((VirtualRunnable)runnable).run(scheduler);
        } else {
          runnable.run();
        }
      } finally {
        nextRunTime = nowInMillis + recurringDelay;
        synchronized (queueLock) {
          taskQueue.add(this);
        }
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * Runnable which handles notifying for locks.
   * 
   * @author jent - Mike Jensen
   */
  protected class WakeUpThread extends OneTimeRunnable {
    protected WakeUpThread(final NotifyObject lock, 
                           final boolean notifyAll, 
                           long delay) {
      super(new Runnable() {
        @Override
        public void run() {
          if (notifyAll) {
            lock.wakeUpAll();
          } else {
            lock.wakeUp();
          }
          
          // wait for waiting thread to be notified and now waiting to take lock
          try {
            lock.waitForWantingToRun();
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
          
          yielding(); // yield this thread so the signaled thread can wake up
          
          /* we must wait for new task to start before we return or
           * the main tick thread may think all tasks are done
           */
          try {
            lock.waitForWakeup();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }, delay, TaskPriority.High);
    }
    
    @Override
    protected void handleDone() {
      // prevent yield call in super class, since we already yielded
    }
  }
  
  /**
   * Object abstraction which notifying occurs through.
   * 
   * @author jent - Mike Jensen
   */
  protected class NotifyObject {
    private final Object obj;
    private volatile boolean wantingToRun;
    private volatile boolean awake;
    
    protected NotifyObject(Object obj) {
      this.obj = obj;
      wantingToRun = false;
      awake = false;
    }
    
    public void wakeUpAll() {
      synchronized (obj) {
        obj.notifyAll();
      }
    }

    public void wakeUp() {
      synchronized (obj) {
        obj.notify();
      }
    }
    
    public void waitForWantingToRun() throws InterruptedException {
      while (! wantingToRun) {
        // spin
      }
    }
    
    public void waitForWakeup() throws InterruptedException {
      while (! awake) {
        // spin
      }
    }
    
    public void yield() throws InterruptedException {
      synchronized (obj) {
        try {
          awake = false;
          wantingToRun = false;
          
          yielding();
          
          obj.wait();
        } finally {
          wantToRun(false, new Runnable() {
            @Override
            public void run() {
              wantingToRun = true;
            }
          });
          
          awake = true;
        }
      }
    }
  }
}
