package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Executor which has no threads itself.  This allows you to have the same scheduler abilities 
 * (schedule tasks, recurring tasks, etc, etc), without having to deal with multiple threads, 
 * memory barriers, or other similar concerns.  This class can be very useful in GUI development 
 * (if you want it to run on the GUI thread).  It also can be useful in android development in a 
 * very similar way.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to 
 * {@link #tick(ExceptionHandler)}.  Since it is running on the calling thread, calls to 
 * {@code Object.wait()} and {@code Thread.sleep()} from sub tasks will block (possibly forever).  
 * The call to {@link #tick(ExceptionHandler)} will not unblock till there is no more work for the 
 * scheduler to currently handle.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class NoThreadScheduler extends AbstractPriorityScheduler {
  protected final Object taskNotifyLock;
  protected final QueueSet queueSet;
  private volatile boolean currentlyBlocking;
  private volatile boolean tickCanceled;
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   */
  public NoThreadScheduler() {
    super(null);
    
    taskNotifyLock = new Object();
    queueSet = new QueueSet(new QueueSetListener() {
      @Override
      public void handleQueueUpdate() {
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
    });
    currentlyBlocking = false;
    tickCanceled = false;
  }

  /**
   * Abstract call to get the value the scheduler should use to represent the current time.  This 
   * can be overridden if someone wanted to artificially change the time.
   * 
   * @param accurate If {@code true} then time estimates are not acceptable
   * @return current time in milliseconds
   */
  protected long nowInMillis(boolean accurate) {
    if (accurate) {
      return Clock.accurateForwardProgressingMillis();
    } else {
      return Clock.lastKnownForwardProgressingMillis();
    }
  }
  
  /**
   * Call to cancel current or the next tick call.  If currently in a 
   * {@link #tick(ExceptionHandler)} call (weather blocking waiting for tasks, or currently running 
   * tasks), this will call the {@link #tick(ExceptionHandler)} to return.  If a task is currently 
   * running it will finish the current task before returning.  If not currently in a 
   * {@link #tick(ExceptionHandler)} call, the next tick call will return immediately without 
   * running anything.
   */
  public void cancelTick() {
    tickCanceled = true;
    
    queueSet.queueListener.handleQueueUpdate();
  }
  
  /**
   * Invoking this will run any tasks which are ready to be run.  This will block as it runs as 
   * many scheduled or waiting tasks as possible.  It is CRITICAL that only one thread at a time 
   * calls the {@link #tick(ExceptionHandler)} OR {@link #blockingTick(ExceptionHandler)}.  While 
   * this class is in general thread safe, if multiple threads invoke either function at the same 
   * time, it is possible a given task may run more than once.  In order to maintain high 
   * performance, threadly does not guard against this condition.
   * 
   * This call allows you to specify an {@link ExceptionHandler}.  If provided, if any tasks throw 
   * an exception, this will be called to communicate the exception.  This allows you to ensure 
   * that you get a returned task count (meaning if provided, no exceptions will be thrown from 
   * this invocation).  If {@code null} is provided for the exception handler, than any tasks 
   * which throw a {@link RuntimeException}, will throw out of this invocation.
   * 
   * This call is NOT thread safe, calling {@link #tick(ExceptionHandler)} or 
   * {@link #blockingTick(ExceptionHandler)} in parallel could cause the same task to be 
   * run multiple times in parallel.
   * 
   * @since 3.2.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick invocation
   */
  public int tick(ExceptionHandler exceptionHandler) {
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
  private int tick(ExceptionHandler exceptionHandler, boolean resetCancelTickIfNoTasksRan) {
    int tasks = 0;
    TaskWrapper nextTask;
    while ((nextTask = getNextReadyTask()) != null && ! tickCanceled) {
      // call will remove task from queue, or reposition as necessary
      if (nextTask.canExecute()) {
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
    }
    
    if ((tasks != 0 || resetCancelTickIfNoTasksRan) && tickCanceled) {
      // reset for future tick calls
      tickCanceled = false;
    }
    
    return tasks;
  }
  
  /**
   * This is similar to {@link #tick(ExceptionHandler)}, except that it will block until there are 
   * tasks ready to run, or until {@link #cancelTick()} is invoked.  
   * 
   * Once there are tasks ready to run, this will continue to block as it runs as many tasks that 
   * are ready to run.  
   * 
   * It is CRITICAL that only one thread at a time calls the {@link #tick(ExceptionHandler)} OR 
   * {@link #blockingTick(ExceptionHandler)}.  
   * 
   * This call allows you to specify an {@link ExceptionHandler}.  If provided, if any tasks throw 
   * an exception, this will be called to communicate the exception.  This allows you to ensure 
   * that you get a returned task count (meaning if provided, no exceptions will be thrown from 
   * this invocation).  If {@code null} is provided for the exception handler, than any tasks 
   * which throw a {@link RuntimeException}, will throw out of this invocation.
   * 
   * This call is NOT thread safe, calling {@link #tick(ExceptionHandler)} or 
   * {@link #blockingTick(ExceptionHandler)} in parallel could cause the same task to be 
   * run multiple times in parallel.
   * 
   * @since 4.0.0
   * 
   * @param exceptionHandler Exception handler implementation to call if any tasks throw an 
   *                           exception, or null to have exceptions thrown out of this call
   * @return quantity of tasks run during this tick invocation
   * @throws InterruptedException thrown if thread is interrupted waiting for task to run
   */
  public int blockingTick(ExceptionHandler exceptionHandler) throws InterruptedException {
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
            TaskWrapper nextTask = queueSet.getNextTask();
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
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    // TODO - handle priority
    OneTimeTaskWrapper result;
    if (delayInMillis == 0) {
      queueSet.addExecute((result = new NoThreadOneTimeTaskWrapper(task, queueSet.executeQueue, 
                                                                   nowInMillis(false))));
    } else {
      queueSet.addScheduled((result = new NoThreadOneTimeTaskWrapper(task, queueSet.scheduleQueue, 
                                                                     nowInMillis(true) + delayInMillis)));
    }
    return result;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay,
                                     TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    // TODO - handle priority
    
    NoThreadRecurringDelayTaskWrapper taskWrapper = 
        new NoThreadRecurringDelayTaskWrapper(task, queueSet, 
                                              nowInMillis(true) + initialDelay, recurringDelay);
    queueSet.addScheduled(taskWrapper);
  }

  @Override
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period,
                                  TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    
    // TODO - handle priority
    
    NoThreadRecurringRateTaskWrapper taskWrapper = 
        new NoThreadRecurringRateTaskWrapper(task, queueSet, 
                                             nowInMillis(true) + initialDelay, period);
    queueSet.addScheduled(taskWrapper);
  }
  
  @Override
  public boolean remove(Runnable task) {
    return queueSet.remove(task);
  }
  
  @Override
  public boolean remove(Callable<?> task) {
    return queueSet.remove(task);
  }

  @Override
  public boolean isShutdown() {
    return false;
  }
  
  /**
   * Call to get the next ready task that is ready to be run.  If there are no tasks, or the next 
   * task still has a remaining delay, this will return {@code null}.
   * 
   * If this is being called in parallel with a {@link #tick(ExecutionHandlerInterface)} call, the 
   * returned task may already be running.  You must check the {@code TaskContainer.running} 
   * boolean if this condition is important to you.
   * 
   * @return next ready task, or {@code null} if there are none
   */
  protected TaskWrapper getNextReadyTask() {
    TaskWrapper tw = queueSet.getNextTask();
    if (tw != null && tw.getScheduleDelay() <= 0) {
      return tw;
    } else {
      return null;
    }
  }
  
  /**
   * Checks if there are tasks ready to be run on the scheduler.  Generally this is called from 
   * the same thread that would call {@link #tick(ExceptionHandler)} (but does not have to be).  
   * If {@link #tick(ExceptionHandler)} is not currently being called, this call indicates if the 
   * next {@link #tick(ExceptionHandler)} will have at least one task to run.  If 
   * {@link #tick(ExceptionHandler)} is currently being invoked, this call will do a best attempt 
   * to indicate if there is at least one more task to run (not including the task which may 
   * currently be running).  It's a best attempt as it will try not to block the thread invoking 
   * {@link #tick(ExceptionHandler)} to prevent it from accepting additional work.
   *  
   * @return {@code true} if there are task waiting to run
   */
  public boolean hasTaskReadyToRun() {
    if (queueSet.executeQueue.isEmpty()) {
      TaskWrapper headTask = queueSet.scheduleQueue.peekFirst();
      return headTask != null && headTask.getScheduleDelay() <= 0;
    } else {
      return true;
    }
  }
  
  /**
   * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running if 
   * {@link #tick(ExceptionHandler)} is being called.  But will avoid additional tasks from being 
   * run on the current {@link #tick(ExceptionHandler)} call.  
   * 
   * If tasks are added concurrently during this invocation they may or may not be removed.
   * 
   * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
   */
  public List<Runnable> clearTasks() {
    ArrayList<TaskWrapper> wrapperList = new ArrayList<TaskWrapper>(queueSet.queueSize());
    queueSet.drainQueueInto(wrapperList);
    wrapperList.trimToSize();
    
    return ContainerHelper.getContainedRunnables(wrapperList);
  }
  
  /**
   * <p>Wrapper for tasks which only executes once.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class NoThreadOneTimeTaskWrapper extends OneTimeTaskWrapper {
    protected NoThreadOneTimeTaskWrapper(Runnable task, 
                                         Queue<? extends TaskWrapper> taskQueue, long runTime) {
      super(task, taskQueue, runTime);
    }
    
    @Override
    public long getScheduleDelay() {
      if (getRunTime() > nowInMillis(false)) {
        return getRunTime() - nowInMillis(true);
      } else {
        return 0;
      }
    }

    @Override
    public void runTask() {
      if (! canceled) {
        // Do not use ExceptionUtils to run task, so that exceptions can be handled in .tick()
        task.run();
      }
    }
  }

  /**
   * <p>Abstract wrapper for any tasks which run repeatedly.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected abstract class NoThreadRecurringTaskWrapper extends RecurringTaskWrapper {
    protected NoThreadRecurringTaskWrapper(Runnable task, QueueSet queueSet, long firstRunTime) {
      super(task, queueSet, firstRunTime);
    }
    
    @Override
    public long getScheduleDelay() {
      if (getRunTime() > nowInMillis(false)) {
        return getRunTime() - nowInMillis(true);
      } else {
        return 0;
      }
    }
    
    /**
     * Called when the implementing class should update the variable {@code nextRunTime} to be the 
     * next absolute time in milliseconds the task should run.
     */
    @Override
    protected abstract void updateNextRunTime();

    @Override
    public void runTask() {
      if (canceled) {
        return;
      }
      
      try {
        // Do not use ExceptionUtils to run task, so that exceptions can be handled in .tick()
        task.run();
      } finally {
        if (! canceled) {
          updateNextRunTime();
          // now that nextRunTime has been set, resort the queue
          queueSet.reschedule(this);
          
          // only set executing to false AFTER rescheduled
          executing = false;
        }
      }
    }
  }
  
  /**
   * <p>Container for tasks which run with a fixed delay after the previous run.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected class NoThreadRecurringDelayTaskWrapper extends NoThreadRecurringTaskWrapper {
    protected final long recurringDelay;
    
    protected NoThreadRecurringDelayTaskWrapper(Runnable task, QueueSet queueSet, 
                                                long firstRunTime, long recurringDelay) {
      super(task, queueSet, firstRunTime);
      
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime = nowInMillis(true) + recurringDelay;
    }
  }
  
  /**
   * <p>Wrapper for tasks which run at a fixed period (regardless of execution time).</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected class NoThreadRecurringRateTaskWrapper extends NoThreadRecurringTaskWrapper {
    protected final long period;
    
    protected NoThreadRecurringRateTaskWrapper(Runnable task, QueueSet queueSet, 
                                               long firstRunTime, long period) {
      super(task, queueSet, firstRunTime);
      
      this.period = period;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
  }
}
