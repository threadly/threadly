package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionHandlerInterface;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.ListUtils;

/**
 * <p>Executor which has no threads itself.  This allows you to have the same scheduler abilities 
 * (schedule tasks, recurring tasks, etc, etc), without having to deal with multiple threads, 
 * memory barriers, or other similar concerns.  This class can be very useful in GUI development 
 * (if you want it to run on the GUI thread).  It also can be useful in android development in a 
 * very similar way.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to {@link #tick()}.  
 * Since it is running on the calling thread, calls to {@code Object.wait()} and 
 * {@code Thread.sleep()} from sub tasks will block (possibly forever).  The call to 
 * {@link #tick(ExceptionHandlerInterface)} will not unblock till there is no more work for the 
 * scheduler to currently handle.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class NoThreadScheduler extends AbstractSubmitterScheduler 
                               implements SchedulerServiceInterface {
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  protected final boolean tickBlocksTillAvailable;
  protected final Object taskNotifyLock;
  protected final Object executeQueueRemoveLock;
  protected final ConcurrentLinkedQueue<OneTimeTask> executeQueue;
  protected final ConcurrentArrayList<TaskContainer> scheduledQueue;
  private volatile boolean tickCanceled;  
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   * 
   * @param tickBlocksTillAvailable {@code true} if calls to {@link #tick()} should block till there is something to run
   */
  public NoThreadScheduler(boolean tickBlocksTillAvailable) {
    this.tickBlocksTillAvailable = tickBlocksTillAvailable;
    taskNotifyLock = new Object();
    executeQueueRemoveLock = new Object();
    executeQueue = new ConcurrentLinkedQueue<OneTimeTask>();
    scheduledQueue = new ConcurrentArrayList<TaskContainer>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
    tickCanceled = false;
  }

  /**
   * Abstract call to get the value the scheduler should use to represent the current time.  This 
   * can be overridden if someone wanted to artificially change the time.
   * 
   * @return current time in milliseconds
   */
  protected long nowInMillis() {
    return ClockWrapper.getSemiAccurateMillis();
  }
  
  /**
   * Call to cancel current or the next tick call.  If currently in a {@link #tick()} call 
   * (weather blocking waiting for tasks, or currently running tasks), this will call the 
   * {@link #tick()} to return.  If a task is currently running it will finish the current task 
   * before returning.  If not currently in a {@link #tick()} call, the next tick call will return 
   * immediately without running anything.
   */
  public void cancelTick() {
    tickCanceled = true;
    
    notifyQueueUpdate();
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs as many scheduled or 
   * waiting tasks as possible.  It is CRITICAL that only one thread at a time calls the 
   * {@link #tick()} function.  While this class is in general thread safe, if multiple threads 
   * call {@link #tick()} at the same time, it is possible a given task may run more than once.  
   * In order to maintain high performance, threadly does not guard against this condition.
   * 
   * Depending on how this class was constructed, this may or may not block if there are no tasks 
   * to run yet.
   * 
   * If any tasks throw a {@link RuntimeException}, they will be bubbled up to this tick call.  
   * Any tasks past that task will not run till the next call to tick.  So it is important that 
   * the implementor handle those exceptions.  
   * 
   * This call is NOT thread safe, calling tick in parallel could cause the same task to be run 
   * multiple times in parallel.
   * 
   * @deprecated please use {@link #tick(ExceptionHandlerInterface)}, providing null for the 
   *               {@link ExceptionHandlerInterface}.  This will be removed in 4.0.0
   * 
   * @return quantity of tasks run during this tick invocation
   * @throws InterruptedException thrown if thread is interrupted waiting for task to run
   *           (this can only throw if constructed with a {@code true} to allow blocking)
   */
  @Deprecated
  public int tick() throws InterruptedException {
    return tick(null);
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs as many scheduled or 
   * waiting tasks as possible.  It is CRITICAL that only one thread at a time calls the 
   * {@link #tick()} function.  While this class is in general thread safe, if multiple threads 
   * call {@link #tick()} at the same time, it is possible a given task may run more than once.  
   * In order to maintain high performance, threadly does not guard against this condition.
   * 
   * Depending on how this class was constructed, this may or may not block if there are no tasks 
   * to run yet.
   * 
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to inform them of the exception.  This allows 
   * you to ensure that you get a returned task count (meaning if provided, no exceptions except 
   * a possible {@link InterruptedException} can be thrown).  If null is provided for the 
   * exception handler, than any tasks which throw a {@link RuntimeException}, will throw out of 
   * this invocation.
   * 
   * This call is NOT thread safe, calling tick in parallel could cause the same task to be run 
   * multiple times in parallel.
   * 
   * @since 3.2.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick invocation
   * @throws InterruptedException thrown if thread is interrupted waiting for task to run
   *           (this can only throw if constructed with a {@code true} to allow blocking)
   */
  public int tick(ExceptionHandlerInterface exceptionHandler) throws InterruptedException {
    int tasks = 0;
    while (true) {  // will break from loop at bottom
      TaskContainer nextTask;
      while ((nextTask = getNextTask(true)) != null && ! tickCanceled) {
        // call will remove task from queue, or reposition as necessary
        try {
          nextTask.runTask();
        } catch (Throwable t) {
          if (exceptionHandler != null) {
            exceptionHandler.handleException(t);
          } else {
            throw ExceptionUtils.makeRuntime(t);
          }
        }

        tasks++;
      }
      
      if (tickBlocksTillAvailable && tasks == 0) {
        synchronized (taskNotifyLock) {
          /* we must check the cancelTick once we have the lock 
           * since that is when the .notify() would happen.
           */
          if (tickCanceled) {
            break;
          }
          nextTask = getNextTask(false);
          if (nextTask == null) {
            taskNotifyLock.wait();
          } else {
            long nextTaskDelay = nextTask.getDelayInMillis();
            if (nextTaskDelay > 0) {
              taskNotifyLock.wait(nextTaskDelay);
            }
          }
        }
      } else {
        /* we are ready to return from call, either because we 
         * ran at least one task, don't want to block, or the 
         * tick call was canceled.
         */
        break;
      }
    }
    
    if (tickCanceled) {
      // reset for future tick calls
      tickCanceled = false;
    }
    
    return tasks;
  }

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    OneTimeTask taskWrapper = new OneTimeTask(task, delayInMillis);
    if (delayInMillis == 0) {
      addImmediateExecute(taskWrapper);
    } else {
      addScheduled(taskWrapper);
    }
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    RecurringDelayTask taskWrapper = new RecurringDelayTask(task, initialDelay, recurringDelay);
    addScheduled(taskWrapper);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    
    RecurringRateTask taskWrapper = new RecurringRateTask(task, initialDelay, period);
    addScheduled(taskWrapper);
  }
  
  /**
   * Notifies that the queue has been updated.  Thus allowing any threads blocked on the 
   * {@code taskNotifyLock} to wake up and check for new work to run. 
   */
  protected void notifyQueueUpdate() {
    // only need to notify if we may possibly be waiting on lock
    if (tickBlocksTillAvailable) {
      synchronized (taskNotifyLock) {
        taskNotifyLock.notify();
      }
    }
  }
  
  /**
   * Adds a task to the simple execute queue.  This avoids locking for adding into the queue.
   * 
   * @param runnable Task to execute on next {@link #tick(ExceptionHandlerInterface)} call
   */
  protected void addImmediateExecute(OneTimeTask runnable) {
    executeQueue.add(runnable);
    
    notifyQueueUpdate();
  }

  /**
   * Adds a task to scheduled/recurring queue.  This call is more expensive than 
   * {@link #addImmediateExecute(OneTimeTask)}, but is necessary for any tasks which are either 
   * delayed or recurring.
   * 
   * @param runnable Task to execute on next {@link #tick(ExceptionHandlerInterface)} call
   */
  protected void addScheduled(TaskContainer runnable) {
    synchronized (scheduledQueue.getModificationLock()) {
      ClockWrapper.stopForcingUpdate();
      try {
        int insertionIndex = ListUtils.getInsertionEndIndex(scheduledQueue, runnable, true);
          
        scheduledQueue.add(insertionIndex, runnable);
      } finally {
        ClockWrapper.resumeForcingUpdate();
      }
    }

    notifyQueueUpdate();
  }
  
  @Override
  public boolean remove(Runnable task) {
    synchronized (executeQueueRemoveLock) {
      if (ContainerHelper.remove(executeQueue, task)) {
        return true;
      }
    }
    synchronized (scheduledQueue.getModificationLock()) {
      return ContainerHelper.remove(scheduledQueue, task);
    }
  }
  
  @Override
  public boolean remove(Callable<?> task) {
    synchronized (executeQueueRemoveLock) {
      if (ContainerHelper.remove(executeQueue, task)) {
        return true;
      }
    }
    synchronized (scheduledQueue.getModificationLock()) {
      return ContainerHelper.remove(scheduledQueue, task);
    }
  }

  @Override
  public boolean isShutdown() {
    return false;
  }
  
  /**
   * Call to get the next task that is ready to be run.  If there are no tasks, or the next task 
   * still has a remaining delay, this will return {@code null}.
   * 
   * If this is being called in parallel with a {@link #tick(ExecutionHandlerInterface)} call, the 
   * returned task may already be running.  You must check the {@code TaskContainer.running} 
   * boolean if this condition is important to you.
   * 
   * @param onlyReturnReadyTask {@code false} to return scheduled tasks which may not be ready for execution
   * @return next ready task, or {@code null} if there are none
   */
  protected TaskContainer getNextTask(boolean onlyReturnReadyTask) {
    TaskContainer nextScheduledTask = scheduledQueue.peekFirst();
    TaskContainer nextExecuteTask = executeQueue.peek();
    if (nextExecuteTask != null) {
      if (nextScheduledTask != null) {
        long scheduleDelay;
        long executeDelay;
        ClockWrapper.stopForcingUpdate();
        try {
          scheduleDelay = nextScheduledTask.getDelayInMillis();
          executeDelay = nextExecuteTask.getDelayInMillis();
        } finally {
          ClockWrapper.resumeForcingUpdate();
        }
        if (scheduleDelay < executeDelay) {
          return nextScheduledTask;
        } else {
          return nextExecuteTask;
        }
      } else {
        return nextExecuteTask;
      }
    } else if (! onlyReturnReadyTask || 
                 (nextScheduledTask != null && nextScheduledTask.getDelayInMillis() <= 0)) {
      return nextScheduledTask;
    } else {
      return null;
    }
  }
  
  /**
   * Checks if there are tasks ready to be run on the scheduler.  Generally this is called from 
   * the same thread that would call .tick() (but does not have to be).  If 
   * {@link #tick(ExceptionHandlerInterface)} is not currently being called, this call indicates 
   * if the next {@link #tick(ExceptionHandlerInterface)} will have at least one task to run.  If 
   * {@link #tick(ExceptionHandlerInterface)} is currently running, this call will indicate if 
   * there is at least one more task to run (not including the task which may currently be 
   * running).  
   * 
   * Calling this does require us to lock the queues to ensure things are not removed while we 
   * check if there is a task to run.
   *  
   * @return {@code true} if there are task waiting to run.
   */
  public boolean hasTaskReadyToRun() {
    synchronized (executeQueueRemoveLock) {
      TaskContainer nextExecuteTask = executeQueue.peek();
      if (nextExecuteTask != null) {
        if (! nextExecuteTask.running) {
          return true;
        } else if (executeQueue.size() > 1) {
          return true;
        }
      }
    }
    
    synchronized (scheduledQueue.getModificationLock()) {
      Iterator<TaskContainer> it = scheduledQueue.iterator();
      while (it.hasNext()) {
        TaskContainer scheduledTask = it.next();
        if (scheduledTask.getDelayInMillis() <= 0 && ! scheduledTask.running) {
          return true;
        }
      }
    }
    
    return false;
  }
  
  /**
   * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running if 
   * {@link #tick()} is being called.  But will avoid additional tasks from being run on the 
   * current {@link #tick()} call.
   * 
   * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
   */
  public List<Runnable> clearTasks() {
    synchronized (scheduledQueue.getModificationLock()) {
      synchronized (executeQueueRemoveLock) {
        List<TaskContainer> containers = new ArrayList<TaskContainer>(executeQueue.size() + 
                                                                        scheduledQueue.size());
        
        Iterator<? extends TaskContainer> it = executeQueue.iterator();
        while (it.hasNext()) {
          TaskContainer tc = it.next();
          if (! tc.running) {
            int index = ListUtils.getInsertionEndIndex(containers, tc, true);
            containers.add(index, tc);
          }
        }
        executeQueue.clear();
        
        it = scheduledQueue.iterator();
        while (it.hasNext()) {
          TaskContainer tc = it.next();
          if (! tc.running) {
            int index = ListUtils.getInsertionEndIndex(containers, tc, true);
            containers.add(index, tc);
          }
        }
        scheduledQueue.clear();
        
        List<Runnable> result = new ArrayList<Runnable>(containers.size());
        it = containers.iterator();
        while (it.hasNext()) {
          result.add(it.next().runnable);
        }
        
        return result;
      }
    }
  }
  
  /**
   * <p>Container abstraction to hold runnables for scheduler.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract static class TaskContainer extends AbstractDelayed 
                                                implements RunnableContainerInterface {
    protected final Runnable runnable;
    protected volatile boolean running;
    
    protected TaskContainer(Runnable runnable) {
      this.runnable = runnable;
      this.running = false;
    }

    @Override
    public Runnable getContainedRunnable() {
      return runnable;
    }
    
    protected void runTask() {
      running = true;
      if (! prepareForRun()) {
        return;
      }
      try {
        runnable.run();
      } finally {
        runComplete();
        running = false;
      }
    }
    
    /**
     * Called before the task starts.  Allowing the container to do any pre-execution tasks 
     * necessary, and give a last chance to avoid execution (for example if the task has been 
     * canceled or removed).
     * 
     * @return {@code true} if execution should continue
     */
    protected boolean prepareForRun() {
      // nothing by default, override to handle
      return true;
    }

    /**
     * Called after the task completes, weather an exception was thrown or it exited normally.
     */
    protected void runComplete() {
      // nothing by default, override to handle
    }
    
    /**
     * Call to get the delay till execution in milliseconds.
     * 
     * @return number of milliseconds to wait before executing task
     */
    protected abstract long getDelayInMillis();
    
    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(getDelayInMillis(), 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * <p>Runnable container for runnables that only run once with an optional delay.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class OneTimeTask extends TaskContainer {
    protected final long delay;
    protected final long runTime;
    
    public OneTimeTask(Runnable runnable, long delay) {
      super(runnable);
      
      this.delay = delay;
      this.runTime = nowInMillis() + delay;
    }
    
    @Override
    protected boolean prepareForRun() {
      boolean allowRun;
      // can be removed since this is a one time task
      if (delay == 0) {
        synchronized (executeQueueRemoveLock) {
          allowRun = executeQueue.remove(this);
        }
      } else {
        allowRun = scheduledQueue.removeFirstOccurrence(this);
      }
      
      return allowRun;
    }

    @Override
    public long getDelayInMillis() {
      return runTime - nowInMillis();
    }
  }
  
  /**
   * <p>Container for runnables which run multiple times.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected abstract class RecurringTask extends TaskContainer {
    protected final long initialDelay;
    protected long nextRunTime;
    
    public RecurringTask(Runnable runnable, long initialDelay) {
      super(runnable);
      
      this.initialDelay = initialDelay;
      nextRunTime = nowInMillis() + initialDelay;
    }

    /**
     * Called when the implementing class should update the variable {@code nextRunTime} to be the 
     * next absolute time in milliseconds the task should run.
     */
    protected abstract void updateNextRunTime();
    
    @Override
    public void runComplete() {
      synchronized (scheduledQueue.getModificationLock()) {
        ClockWrapper.stopForcingUpdate();
        try {
          updateNextRunTime();
          
          // almost certainly will be the first item in the queue
          int currentIndex = scheduledQueue.indexOf(this);
          if (currentIndex < 0) {
            // task was removed from queue, do not re-insert
            return;
          }
          long nextDelay = getDelayInMillis();
          if (nextDelay < 0) {
            nextDelay = 0;
          }
          int insertionIndex = ListUtils.getInsertionEndIndex(scheduledQueue, nextDelay, true);
          
          scheduledQueue.reposition(currentIndex, insertionIndex);
        } finally {
          ClockWrapper.resumeForcingUpdate();
        }
      }
    }

    @Override
    public long getDelayInMillis() {
      return nextRunTime - nowInMillis();
    }
  }
  
  /**
   * <p>Container for runnables which run with a fixed delay after the previous run.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringDelayTask extends RecurringTask {
    protected final long recurringDelay;
    
    public RecurringDelayTask(Runnable runnable, long initialDelay, long recurringDelay) {
      super(runnable, initialDelay);
      
      this.recurringDelay = recurringDelay;
    }

    @Override
    protected void updateNextRunTime() {
      nextRunTime = nowInMillis() + recurringDelay;
    }
  }
  
  /**
   * <p>Container for runnables which run with a fixed rate, regardless of execution time.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecurringRateTask extends RecurringTask {
    protected final long period;
    
    public RecurringRateTask(Runnable runnable, long initialDelay, long period) {
      super(runnable, initialDelay);
      
      this.period = period;
    }

    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
  }
}
