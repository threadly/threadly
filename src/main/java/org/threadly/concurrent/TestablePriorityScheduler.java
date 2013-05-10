package org.threadly.concurrent;

import java.lang.Thread.State;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.TestableLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.util.Clock;
import org.threadly.util.ListUtils;

/**
 * Scheduler which is designed to be used during unit testing.
 * Although it actually runs multiple threads, it only has one 
 * thread actively executing at a time (thus simulating single threaded).
 * 
 * The scheduler uses .awaits() and .sleep()'s to TestableLock's as opportunities
 * to simulate multiple threads.  When you call .tick() you progress forward
 * externally scheduled threads, or possibly threads which are sleeping.
 * 
 * @author jent - Mike Jensen
 */
public class TestablePriorityScheduler implements PrioritySchedulerInterface, 
                                                  LockFactory {
  private final PrioritySchedulerInterface scheduler;
  private final LinkedList<RunnableContainer> taskQueue;
  private final LinkedList<TestableLock> waitingThreads;
  private final Object queueLock;
  private final Object actionLock;
  private volatile Thread runningThread;
  private long nowInMillis;

  /**
   * Constructs a new TestablePriorityScheduler with the backed thread pool.
   * Because this only simulates threads running in a single threaded way, 
   * it must have a sufficiently large thread pool to back that.
   * 
   * @param scheduler scheduler which will be used as new threads are necessary
   */
  public TestablePriorityScheduler(PrioritySchedulerInterface scheduler) {
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide backing scheduler");
    }
    
    this.scheduler = scheduler;
    taskQueue = new LinkedList<RunnableContainer>();
    waitingThreads = new LinkedList<TestableLock>();
    queueLock = new Object();
    actionLock = new Object();
    runningThread = null;
    nowInMillis = Clock.accurateTime();
  }
  
  @Override
  public void execute(Runnable task) {
    schedule(task, 0, scheduler.getDefaultPriority());
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, scheduler.getDefaultPriority());
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
                           scheduler.getDefaultPriority());
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    add(new RecurringRunnable(task, initialDelay, 
                              recurringDelay, priority));
  }
  
  private void add(RunnableContainer runnable) {
    synchronized (queueLock) {
      int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, runnable);
      
      taskQueue.add(insertionIndex, runnable);
    }
  }

  @Override
  public boolean isShutdown() {
    return false;
  }
  
  private void setRunningThread() {
    if (runningThread != null) {
      throw new IllegalStateException(System.nanoTime() + " - " + "Another thread is already running: " + runningThread);
    }
    new Exception(System.nanoTime() + " - " + "Thread is now running: " + Thread.currentThread()).printStackTrace();
    runningThread = Thread.currentThread();
  }
  
  private void waitForThreadToIdle() {
    if (runningThread == null) {
      throw new IllegalStateException(System.nanoTime() + " - " + "No set running thread");
    }
    while (runningThread.getState() != State.WAITING) {
      System.out.println(System.nanoTime() + " - " + "Waiting for previous thread to go to wait: " + runningThread.getState());
    }
    new Exception(System.nanoTime() + " - " + "Unsetting running thread: " + runningThread).printStackTrace();
    runningThread = null;
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
  
  /**
   * This ticks forward one step based off the current time of calling.  
   * It runs as many events are ready for the time provided, and will block
   * until all those events have completed (or are waiting or sleeping). 
   * 
   * @param currentTime time reference for the scheduler to use to look for waiting events 
   * @return qty of steps taken forward.  Returns zero if no events to run for provided time.
   */
  public int tick(long currentTime) {
    System.out.println(System.nanoTime() + " - " + "Tick called with time: " + currentTime);
    if (currentTime < nowInMillis) {
      throw new IllegalArgumentException("Can not go backwards in time");
    }
    
    nowInMillis = currentTime;
    
    int ranTasks = 0;
    
    RunnableContainer nextTask = getNextTask(currentTime);
    while (nextTask != null) {
      ranTasks++;
      try {
        handleTask(nextTask);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      nextTask = getNextTask(currentTime);
    }
    
    return ranTasks;
  }
  
  private RunnableContainer getNextTask(long nowInMs) {
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
    System.out.println(System.nanoTime() + " - " + "Handling task: " + nextTask + " - " + nextTask.runnable);
    synchronized (actionLock) {
      scheduler.execute(nextTask);
      
      actionLock.wait();
      System.out.println(System.nanoTime() + " - " + "handle task woken up" + " - " + nextTask.runnable);
      waitForThreadToIdle();
    }
  }

  @Override
  public VirtualLock makeLock() {
    return new TestableLock(this);
  }

  /**
   * should only be called from TestableVirtualLock
   * 
   * @param lock lock referencing calling into scheduler
   * @throws InterruptedException
   */
  @SuppressWarnings("javadoc")
  public void waiting(TestableLock lock) throws InterruptedException {
    if (Thread.currentThread() != runningThread) {
      throw new IllegalStateException("Only running thread should call into here");
    }
    synchronized (lock) {
      // maybe start a new task
      synchronized (actionLock) {
        //System.out.println("About to notify action lock for wait");
        actionLock.notify();
      }
      
      try {
        waitingThreads.addLast(lock);
        lock.wait();
      } finally {
        waitForThreadToIdle();
        setRunningThread();
        waitingThreads.remove(lock);
      }
    }
  }

  /**
   * should only be called from TestableVirtualLock
   * 
   * @param lock lock referencing calling into scheduler
   * @param waitTimeInMs time to wait on lock
   * @throws InterruptedException
   */
  @SuppressWarnings("javadoc")
  public void waiting(final TestableLock lock, 
                      long waitTimeInMs) throws InterruptedException {
    final boolean[] notified = new boolean[] { false };
    // schedule runnable to wake up in case not signaled
    schedule(new Runnable() {
      @Override
      public void run() {
        if (! notified[0]) {
          signal(lock);
        }
      }
    }, waitTimeInMs);
    
    waiting(lock);
    notified[0] = true;
  }

  /**
   * should only be called from TestableVirtualLock
   * 
   * @param lock lock referencing calling into scheduler
   */
  public void signal(TestableLock lock) {
    synchronized (lock) {
      if (waitingThreads.contains(lock)) {
        // schedule task to wake up other thread
        add(new WakeUpThread(lock, 0));
      }
    }
  }

  /**
   * should only be called from TestableVirtualLock
   * 
   * @param lock lock referencing calling into scheduler
   */
  public void signalAll(TestableLock lock) {
    Iterator<TestableLock> it = waitingThreads.iterator();
    while (it.hasNext()) {
      if (it.next().equals(lock)) {
        add(new WakeUpThread(lock, 0));
      }
    }
  }

  /**
   * should only be called from TestableVirtualLock or 
   * the running thread inside the scheduler
   * 
   * @param sleepTime time for thread to sleep
   * @throws InterruptedException
   */
  @SuppressWarnings("javadoc")
  public void sleep(long sleepTime) throws InterruptedException {
    if (Thread.currentThread() != runningThread) {
      throw new IllegalStateException("Only running thread should call into here");
    }
    Object sleepLock = new Object();
    synchronized (sleepLock) {
      synchronized (actionLock) {
        add(new WakeUpThread(sleepLock, sleepTime));
        
        System.out.println(System.nanoTime() + " - " + "About to notify actionLock for sleep");
        actionLock.notify();
      }
      
      sleepLock.wait();
      waitForThreadToIdle();
      setRunningThread();
    }
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }
  
  private abstract class RunnableContainer implements Runnable, Delayed {
    protected final Runnable runnable;
    protected final TaskPriority priority;
    protected final Exception creationStack;
    
    protected RunnableContainer(Runnable runnable, TaskPriority priority) {
      this.runnable = runnable;
      this.priority = priority;
      creationStack = new Exception();
    }
    
    @Override
    public void run() {
      run(TestablePriorityScheduler.this);
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
  
  private class OneTimeRunnable extends RunnableContainer {
    private final long runTime;
    
    private OneTimeRunnable(Runnable runnable, long delay, 
                            TaskPriority priority) {
      super(runnable, priority);
      
      this.runTime = nowInMillis + delay;
    }
    
    @Override
    public void run(LockFactory scheduler) {
      System.out.println(System.nanoTime() + " - " + "Starting: " + this + " - " + runnable);
      //creationStack.printStackTrace();
      setRunningThread();
      if (runnable instanceof VirtualRunnable) {
        ((VirtualRunnable)runnable).run(scheduler);
      } else {
        runnable.run();
      }

      synchronized (actionLock) {
        if (Thread.currentThread() != runningThread) {
          throw new IllegalStateException("Only running thread should call into here");
        }
        actionLock.notify();
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(runTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  private class RecurringRunnable extends RunnableContainer {
    private final long recurringDelay;
    private long nextRunTime;
    
    public RecurringRunnable(Runnable runnable, 
                             long initialDelay, 
                             long recurringDelay, 
                             TaskPriority priority) {
      super(runnable, priority);
      
      this.recurringDelay = recurringDelay;
      nextRunTime = nowInMillis + initialDelay;
    }
    
    @Override
    public void run(LockFactory scheduler) {
      System.out.println("Starting: " + this);
      //creationStack.printStackTrace();
      setRunningThread();
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
      
      synchronized (actionLock) {
        if (Thread.currentThread() != runningThread) {
          throw new IllegalStateException("Only running thread should call into here");
        }
        actionLock.notify();
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  private class WakeUpThread extends OneTimeRunnable {
    private WakeUpThread(final Object lock, long delay) {
      super(new Runnable() {
        @Override
        public void run() {
          synchronized (lock) {
            lock.notify();
          }
        }
      }, delay, TaskPriority.High);
    }
  }
}
