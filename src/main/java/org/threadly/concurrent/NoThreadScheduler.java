package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandlerInterface;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Executor which has no threads itself.  This allows you to have the same scheduler abilities 
 * (schedule tasks, recurring tasks, etc, etc), without having to deal with multiple threads, 
 * memory barriers, or other similar concerns.  This class can be very useful in GUI development 
 * (if you want it to run on the GUI thread).  It also can be useful in android development in a 
 * very similar way.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to 
 * {@link #tick(ExceptionHandlerInterface)}.  Since it is running on the calling thread, calls to 
 * {@code Object.wait()} and {@code Thread.sleep()} from sub tasks will block (possibly forever).  
 * The call to {@link #tick(ExceptionHandlerInterface)} will not unblock till there is no more 
 * work for the scheduler to currently handle.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class NoThreadScheduler extends AbstractSubmitterScheduler 
                               implements SchedulerServiceInterface {
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  protected final Object taskNotifyLock;
  protected final ConcurrentLinkedQueue<OneTimeTask> executeQueue;
  protected final ConcurrentArrayList<TaskContainer> scheduledQueue;
  private volatile boolean currentlyBlocking;
  private volatile boolean tickCanceled;
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   */
  public NoThreadScheduler() {
    taskNotifyLock = new Object();
    executeQueue = new ConcurrentLinkedQueue<OneTimeTask>();
    scheduledQueue = new ConcurrentArrayList<TaskContainer>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
    currentlyBlocking = false;
    tickCanceled = false;
  }

  /**
   * Abstract call to get the value the scheduler should use to represent the current time.  This 
   * can be overridden if someone wanted to artificially change the time.
   * 
   * @return current time in milliseconds
   */
  protected long nowInMillis() {
    return Clock.accurateForwardProgressingMillis();
  }
  
  /**
   * Call to cancel current or the next tick call.  If currently in a 
   * {@link #tick(ExceptionHandlerInterface)} call (weather blocking waiting for tasks, or 
   * currently running tasks), this will call the {@link #tick(ExceptionHandlerInterface)} to 
   * return.  If a task is currently running it will finish the current task before returning.  If 
   * not currently in a {@link #tick(ExceptionHandlerInterface)} call, the next tick call will 
   * return immediately without running anything.
   */
  public void cancelTick() {
    tickCanceled = true;
    
    notifyQueueUpdate();
  }
  
  /**
   * Invoking this will run any tasks which are ready to be run.  This will block as it runs as 
   * many scheduled or waiting tasks as possible.  It is CRITICAL that only one thread at a time 
   * calls the {@link #tick(ExceptionHandlerInterface)} OR 
   * {@link #blockingTick(ExceptionHandlerInterface)}.  While this class is in general thread 
   * safe, if multiple threads invoke either function at the same time, it is possible a given 
   * task may run more than once.  In order to maintain high performance, threadly does not guard 
   * against this condition.
   * 
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to communicate the exception.  This allows you 
   * to ensure that you get a returned task count (meaning if provided, no exceptions will be 
   * thrown from this invocation).  If {@code null} is provided for the exception handler, than 
   * any tasks which throw a {@link RuntimeException}, will throw out of this invocation.
   * 
   * This call is NOT thread safe, calling {@link #tick(ExceptionHandlerInterface)} or 
   * {@link #blockingTick(ExceptionHandlerInterface)} in parallel could cause the same task to be 
   * run multiple times in parallel.
   * 
   * @since 3.2.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick invocation
   */
  public int tick(ExceptionHandlerInterface exceptionHandler) {
    return tick(exceptionHandler, true);
  }
  
  /**
   * Internal tick implementation.  Allowing control on if the cancelTick boolean should be reset 
   * if no tasks are run.  Thus allowing for an optimistic attempt to run tasks, while maintaining 
   * the cancelTick state.
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @param resetCancelTickIfNoTasksRan if {@code true} will reset cancelTick weather tasks ran or 
   *                                      not, otherwise cancelTick will only be reset if tasks ran 
   * @return quantity of tasks run during this tick invocation
   */
  private int tick(ExceptionHandlerInterface exceptionHandler, boolean resetCancelTickIfNoTasksRan) {
    int tasks = 0;
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
    
    if ((tasks != 0 || resetCancelTickIfNoTasksRan) && tickCanceled) {
      // reset for future tick calls
      tickCanceled = false;
    }
    
    return tasks;
  }
  
  /**
   * This is similar to {@link #tick(ExceptionHandlerInterface)}, except that it will block until 
   * there are tasks ready to run, or until {@link #cancelTick()} is invoked.  
   * 
   * Once there are tasks ready to run, this will continue to block as it runs as many tasks that 
   * are ready to run.  
   * 
   * It is CRITICAL that only one thread at a time calls the 
   * {@link #tick(ExceptionHandlerInterface)} OR {@link #blockingTick(ExceptionHandlerInterface)}.  
   * 
   * This call allows you to specify an {@link ExceptionHandlerInterface}.  If provided, if any 
   * tasks throw an exception, this will be called to communicate the exception.  This allows you 
   * to ensure that you get a returned task count (meaning if provided, no exceptions will be 
   * thrown from this invocation).  If {@code null} is provided for the exception handler, than 
   * any tasks which throw a {@link RuntimeException}, will throw out of this invocation.
   * 
   * This call is NOT thread safe, calling {@link #tick(ExceptionHandlerInterface)} or 
   * {@link #blockingTick(ExceptionHandlerInterface)} in parallel could cause the same task to be 
   * run multiple times in parallel.
   * 
   * @since 4.0.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick invocation
   * @throws InterruptedException thrown if thread is interrupted waiting for task to run
   */
  public int blockingTick(ExceptionHandlerInterface exceptionHandler) throws InterruptedException {
    int initialTickResult = tick(exceptionHandler, false);
    if (initialTickResult == 0) {
      currentlyBlocking = true;
      try {
        synchronized (taskNotifyLock) {
          while (true) {
            /* we must check the cancelTick once we have the lock 
             * since that is when the .notify() would happen.
             */
            if (tickCanceled) {
              tickCanceled = false;
              return 0;
            }
            TaskContainer nextTask = getNextTask(false);
            if (nextTask == null) {
              taskNotifyLock.wait();
            } else {
              long nextTaskDelay = nextTask.getScheduleDelay();
              if (nextTaskDelay > 0) {
                taskNotifyLock.wait(nextTaskDelay);
              } else {
                // task is ready to run, so break loop
                break;
              }
            }
          }
        }
      } finally {
        currentlyBlocking = false;
      }
      
      return tick(exceptionHandler, true);
    } else {
      return initialTickResult;
    }
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
   * 
   *  This must be called AFTER the queue has been updated.
   */
  protected void notifyQueueUpdate() {
    /* This is an optimization as we only need to synchronize and notify if there is a blocking 
     * tick.
     * 
     * This works because it is set BEFORE synchronizing in blocking tick, and we are only 
     * notified here AFTER the queue has already been updated.
     * 
     * So if the currentlyBlocking has not been set by the time we check it, but we will block, 
     * then when it synchronizes it should see the queue update that already occured anyways.
     */
    if (currentlyBlocking) {
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
      int insertionIndex = TaskListUtils.getInsertionEndIndex(scheduledQueue, runnable.getRunTime());
      
      scheduledQueue.add(insertionIndex, runnable);
    }

    notifyQueueUpdate();
  }
  
  @Override
  public boolean remove(Runnable task) {
    if (ContainerHelper.remove(executeQueue, task)) {
      return true;
    }
    synchronized (scheduledQueue.getModificationLock()) {
      return ContainerHelper.remove(scheduledQueue, task);
    }
  }
  
  @Override
  public boolean remove(Callable<?> task) {
    if (ContainerHelper.remove(executeQueue, task)) {
      return true;
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
        if (nextScheduledTask.getRunTime() < nextExecuteTask.getRunTime()) {
          return nextScheduledTask;
        } else {
          return nextExecuteTask;
        }
      } else {
        return nextExecuteTask;
      }
    } else if (! onlyReturnReadyTask || 
                 (nextScheduledTask != null && nextScheduledTask.getScheduleDelay() <= 0)) {
      return nextScheduledTask;
    } else {
      return null;
    }
  }
  
  /**
   * Checks if there are tasks ready to be run on the scheduler.  Generally this is called from 
   * the same thread that would call {@link #tick(ExceptionHandlerInterface)} (but does not have 
   * to be).  If {@link #tick(ExceptionHandlerInterface)} is not currently being called, this call 
   * indicates if the next {@link #tick(ExceptionHandlerInterface)} will have at least one task to 
   * run.  If {@link #tick(ExceptionHandlerInterface)} is currently being invoked, this call will 
   * do a best attempt to indicate if there is at least one more task to run (not including the 
   * task which may currently be running).  It's a best attempt as it will try not to block the 
   * thread invoking {@link #tick(ExceptionHandlerInterface)} to prevent it from accepting 
   * additional work.
   *  
   * @return {@code true} if there are task waiting to run
   */
  public boolean hasTaskReadyToRun() {
    while (true) {
      TaskContainer nextExecuteTask = executeQueue.peek();
      if (nextExecuteTask != null) {
        if (nextExecuteTask.running) {
          // loop and retry, should be removed from queue shortly
          Thread.yield();
        } else {
          return true;
        }
      } else {
        break;
      }
    }
    
    synchronized (scheduledQueue.getModificationLock()) {
      Iterator<TaskContainer> it = scheduledQueue.iterator();
      while (it.hasNext()) {
        TaskContainer scheduledTask = it.next();
        if (scheduledTask.running) {
          continue;
        } else if (scheduledTask.getScheduleDelay() <= 0) {
          return true;
        } else {
          return false;
        }
      }
    }
    
    return false;
  }
  
  /**
   * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running if 
   * {@link #tick(ExceptionHandlerInterface)} is being called.  But will avoid additional tasks 
   * from being run on the current {@link #tick(ExceptionHandlerInterface)} call.  
   * 
   * If tasks are added concurrently during this invocation they may or may not be removed.
   * 
   * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
   */
  public List<Runnable> clearTasks() {
    List<TaskContainer> containers;
    synchronized (scheduledQueue.getModificationLock()) {
      containers = new ArrayList<TaskContainer>(executeQueue.size() + 
                                                  scheduledQueue.size());
      
      Iterator<? extends TaskContainer> it = executeQueue.iterator();
      while (it.hasNext()) {
        TaskContainer tc = it.next();
        /* we must use executeQueue.remove(Object) instead of it.remove() 
         * This is to assure it is atomically removed (without executing)
         */
        if (! tc.running && executeQueue.remove(tc)) {
          int index = TaskListUtils.getInsertionEndIndex(containers, tc.getRunTime());
          containers.add(index, tc);
        }
      }
      
      it = scheduledQueue.iterator();
      while (it.hasNext()) {
        TaskContainer tc = it.next();
        if (! tc.running) {
          int index = TaskListUtils.getInsertionEndIndex(containers, tc.getRunTime());
          containers.add(index, tc);
        }
      }
      scheduledQueue.clear();
    }
    
    return ContainerHelper.getContainedRunnables(containers);
  }
  
  /**
   * <p>Container abstraction to hold runnables for scheduler.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract class TaskContainer implements DelayedTaskInterface, 
                                                    RunnableContainerInterface {
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
      if (getRunTime() > nowInMillis()) {
        return getRunTime() - nowInMillis();
      } else {
        return 0;
      }
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
        allowRun = executeQueue.remove(this);
      } else {
        allowRun = scheduledQueue.removeFirstOccurrence(this);
      }
      
      return allowRun;
    }

    @Override
    public long getRunTime() {
      return runTime;
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
        updateNextRunTime();
        
        // almost certainly will be the first item in the queue
        int currentIndex = scheduledQueue.indexOf(this);
        if (currentIndex < 0) {
          // task was removed from queue, do not re-insert
          return;
        }
        int insertionIndex = TaskListUtils.getInsertionEndIndex(scheduledQueue, getRunTime());
        
        scheduledQueue.reposition(currentIndex, insertionIndex);
      }
    }

    @Override
    public long getRunTime() {
      return nextRunTime;
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
