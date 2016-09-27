package org.threadly.concurrent;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
  protected final QueueSetListener queueListener;
  protected final QueueManager queueManager;
  protected final AtomicReference<Thread> blockingThread;
  private volatile boolean tickRunning;
  private volatile boolean tickCanceled;
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   */
  public NoThreadScheduler() {
    this(null, DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler with specified default priority behavior.
   * 
   * @param defaultPriority Default priority for tasks which are submitted without any specified priority
   * @param maxWaitForLowPriorityInMs time low priority tasks to wait if there are high priority tasks ready to run
   */
  public NoThreadScheduler(TaskPriority defaultPriority, long maxWaitForLowPriorityInMs) {
    super(defaultPriority);
    
    queueManager = new QueueManager(queueListener = new QueueSetListener() {
      @Override
      public void handleQueueUpdate() {
        Thread t = blockingThread.get();
        if (t != null) {
          LockSupport.unpark(t);
        }
      }
    }, maxWaitForLowPriorityInMs);
    blockingThread = new AtomicReference<Thread>(null);
    tickRunning = false;
    tickCanceled = false;
    
    // call to verify and set values
    setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
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
    
    queueListener.handleQueueUpdate();
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
   * run multiple times in parallel.  Invoking in parallel will also make the behavior of 
   * {@link #getActiveTaskCount()} non-deterministic and inaccurate.
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
    tickRunning = true;
    try {
      while ((nextTask = getNextReadyTask()) != null && ! tickCanceled) {
        // call will remove task from queue, or reposition as necessary
        // we can cheat with the execution reference since task de-queue is single threaded
        if (nextTask.canExecute(nextTask.getExecuteReference())) {
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
    } finally {
      tickRunning = false;
    }
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
   * run multiple times in parallel.  Invoking in parallel will also make the behavior of 
   * {@link #getActiveTaskCount()} non-deterministic and inaccurate.
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
      Thread currentThread = Thread.currentThread();
      // we already tried to optimistically run something above, so we now must prepare to park
      // and park if we still find nothing to execute
      if (! blockingThread.compareAndSet(null, currentThread)) {
        throw new IllegalStateException("Another thread is already blocking!!");
      }
      try {
        while (true) {
          /* we must check the cancelTick once we have the lock 
           * since that is when the .notify() would happen.
           */
          if (tickCanceled) {
            tickCanceled = false;
            return 0;
          } else if (currentThread.isInterrupted()) {
            throw new InterruptedException();
          }
          TaskWrapper nextTask = queueManager.getNextTask();
          if (nextTask == null) {
              LockSupport.park();
          } else {
            long nextTaskDelay = nextTask.getScheduleDelay();
            if (nextTaskDelay > 0) {
              LockSupport.parkNanos(Clock.NANOS_IN_MILLISECOND * nextTaskDelay);
            } else {
              // task is ready to run, so break loop
              break;
            }
          }
        }
      } finally {
        // lazy set should be safe here as a CAS operation (the only read done for this atomic) 
        // should invoke a volatile write to see the update.  But assuming it does not (since the 
        // docs are not perfectly clear on it), in theory only one thread should be invoking tick 
        // anyways, so the worst case would be another thread sees us as still blocking even 
        // though we have gone into tick (or completed), but that would be an indication of a bad 
        // design pattern with NoThreadScheduler.  If only one thread is invoking tick (as it 
        // should).  This would never be seen.
        blockingThread.lazySet(null);
      }
      
      return tick(exceptionHandler, true);
    } else {
      return initialTickResult;
    }
  }

  @Override
  protected OneTimeTaskWrapper doSchedule(Runnable task, long delayInMillis, TaskPriority priority) {
    QueueSet queueSet = queueManager.getQueueSet(priority);
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
    if (priority == null) {
      priority = defaultPriority;
    }
    
    QueueSet queueSet = queueManager.getQueueSet(priority);
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
    if (priority == null) {
      priority = defaultPriority;
    }
    
    QueueSet queueSet = queueManager.getQueueSet(priority);
    
    NoThreadRecurringRateTaskWrapper taskWrapper = 
        new NoThreadRecurringRateTaskWrapper(task, queueSet, 
                                             nowInMillis(true) + initialDelay, period);
    queueSet.addScheduled(taskWrapper);
  }

  @Override
  public int getActiveTaskCount() {
    return tickRunning ? 1 : 0;
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
    TaskWrapper tw = queueManager.getNextTask();
    if (tw == null || tw.getScheduleDelay() > 0) {
      return null;
    } else {
      return tw;
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
    for (TaskPriority p : TaskPriority.values()) {
      if (hasTaskReadyToRun(queueManager.getQueueSet(p))) {
        return true;
      }
    }
    return false;
  }
  
  private static boolean hasTaskReadyToRun(QueueSet queueSet) {
    if (queueSet.executeQueue.isEmpty()) {
      TaskWrapper headTask = queueSet.scheduleQueue.peekFirst();
      return headTask != null && headTask.getScheduleDelay() <= 0;
    } else {
      return true;
    }
  }
  
  /**
   * Checks how long till the next task will be ready to execute.  If there are no tasks in this 
   * scheduler currently then {@link Long#MAX_VALUE} will be returned.  If there is a task ready 
   * to execute this will return a value less than or equal to zero.  If the task is past its 
   * desired point of execution the result will be a negative amount of milliseconds past that 
   * point in time.  
   * 
   * Generally this is called from the same thread that would invoke 
   * {@link #tick(ExceptionHandler)} (but does not have to be).  Since this does not block or lock 
   * if being invoked in parallel with {@link #tick(ExceptionHandler)}, the results may be no 
   * longer accurate by the time this invocation has returned.
   * 
   * If a recurring task is currently running this will return a very large number.  This will 
   * remain until the task has rescheduled itself.  To avoid this just ensure this is never 
   * invoked in parallel with {@link #tick(ExceptionHandler)}.
   * 
   * This can be useful if you want to know how long you can block on something, ASSUMING you can 
   * detect that something has been added to the scheduler, and interrupt that blocking in order 
   * to handle tasks.
   * 
   * @return Milliseconds till the next task is ready to run
   */
  public long getDelayTillNextTask() {
    TaskWrapper tw = queueManager.getNextTask();
    if (tw != null) {
      return tw.getRunTime() - nowInMillis(true);
    } else {
      return Long.MAX_VALUE;
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
    return queueManager.clearQueue();
  }
  
  @Override
  protected QueueManager getQueueManager() {
    return queueManager;
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
      if (! invalidated) {
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
      if (invalidated) {
        return;
      }
      
      try {
        // Do not use ExceptionUtils to run task, so that exceptions can be handled in .tick()
        task.run();
      } finally {
        if (! invalidated) {
          updateNextRunTime();
          // now that nextRunTime has been set, resort the queue
          queueSet.reschedule(this);  // this will set executing to false atomically with the resort
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
