package org.threadly.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * <p>Abstract implementation for implementations of {@link PrioritySchedulerService}.  In 
 * general this wont be useful outside of Threadly developers, but must be a public interface 
 * since it is used in sub-packages.</p>
 * 
 * <p>If you do find yourself using this class, please post an issue on github to tell us why.  If 
 * there is something you want our schedulers to provide, we are happy to hear about it.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
@SuppressWarnings("deprecation")
public abstract class AbstractPriorityScheduler extends AbstractSubmitterScheduler 
                                                implements PrioritySchedulerInterface {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  // tuned for performance of scheduled tasks
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;

  protected final TaskPriority defaultPriority;
  
  protected AbstractPriorityScheduler(TaskPriority defaultPriority) {
    if (defaultPriority == null) {
      defaultPriority = DEFAULT_PRIORITY;
    }
    this.defaultPriority = defaultPriority;
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
  public abstract void setMaxWaitForLowPriority(long maxWaitForLowPriorityInMs);
  
  /**
   * If a section of code wants a different default priority, or wanting to provide a specific 
   * default priority in for {@link KeyDistributedExecutor}, or {@link KeyDistributedScheduler}.
   * 
   * @param priority default priority for {@link PrioritySchedulerService} implementation
   * @return a {@link PrioritySchedulerService} with the default priority specified
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

  @Override
  protected void doSchedule(Runnable task, long delayInMillis) {
    doSchedule(task, delayInMillis, defaultPriority);
  }

  /**
   * Constructs a {@link OneTimeTaskWrapper} and adds it to the most efficient queue.  If there is 
   * no delay it will use {@link #addToExecuteQueue(OneTimeTaskWrapper)}, if there is a delay it 
   * will be added to {@link #addToScheduleQueue(TaskWrapper)}.
   * 
   * @param task Runnable to be executed
   * @param delayInMillis delay to wait before task is run
   * @param priority Priority for task execution
   * @return Wrapper that was scheduled
   */
  protected abstract OneTimeTaskWrapper doSchedule(Runnable task, 
                                                   long delayInMillis, 
                                                   TaskPriority priority);

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
  public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
    scheduleAtFixedRate(task, initialDelay, period, null);
  }

  protected static TaskWrapper getNextTask(QueueSet highPriorityQueueSet, QueueSet lowPriorityQueueSet, 
                                           long maxWaitForLowPriorityInMs) {
    TaskWrapper nextHighPriorityTask = highPriorityQueueSet.getNextTask();
    TaskWrapper nextLowPriorityTask = lowPriorityQueueSet.getNextTask();
    if (nextLowPriorityTask == null) {
      return nextHighPriorityTask;
    } else if (nextHighPriorityTask == null) {
      return nextLowPriorityTask;
    } else if (nextHighPriorityTask.getRunTime() <= nextLowPriorityTask.getRunTime()) {
      return nextHighPriorityTask;
    } else if (nextHighPriorityTask.getScheduleDelay() > 0 || 
        // before the above check we know the low priority has been waiting longer than the high 
        // priority, but since the high priority is not ready to run, we can just return the low 
        // priority a clock call was invoked IF the high priority task was not already known to 
        // be ready to run
        //
        // OR
        //
        // at this point we know the high task is ready to run
        // but the low priority task has been waiting LONGER (and thus also ready to run)
        // So we will return the low priority task IF it has been waiting over the max wait time
        // At this point there may or may not have been a single clock invocation to check if the 
        // high priority task was ready (if it was known ready, none was invoked)
        // because of that we _may_ have to invoke the clock here
        Clock.lastKnownForwardProgressingMillis() - nextLowPriorityTask.getRunTime() > maxWaitForLowPriorityInMs || 
        Clock.accurateForwardProgressingMillis() - nextLowPriorityTask.getRunTime() > maxWaitForLowPriorityInMs) {
      return nextLowPriorityTask;
    } else {
      // task is ready to run, low priority is also ready, but has not been waiting long enough
      return nextHighPriorityTask;
    }
  }

  /**
   * Returns the {@link QueueSet} for a specified priority.
   * 
   * @param priority Priority that should match to the given {@link QueueSet}
   * @return {@link QueueSet} which matches to the priority
   */
  protected abstract QueueSet getQueueSet(TaskPriority priority);
  
  /**
   * <p>Interface to be notified when relevant changes happen to the queue set.</p>
   * 
   * @since 4.3.0
   */
  protected interface QueueSetListener {
    /**
     * Invoked when the head of the queue set has been updated.  This can be used to wake up 
     * blocking threads waiting for tasks to consume.
     */
    public void handleQueueUpdate();
  }

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
    protected final QueueSetListener queueListener;
    protected final ConcurrentLinkedQueue<OneTimeTaskWrapper> executeQueue;
    protected final ConcurrentArrayList<TaskWrapper> scheduleQueue;
    
    public QueueSet(QueueSetListener queueListener) {
      this.queueListener = queueListener;
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

      queueListener.handleQueueUpdate();
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
        queueListener.handleQueueUpdate();
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
      int insertionIndex = -1;
      synchronized (scheduleQueue.getModificationLock()) {
        int currentIndex = scheduleQueue.lastIndexOf(task);
        if (currentIndex > 0) {
          insertionIndex = TaskListUtils.getInsertionEndIndex(scheduleQueue, task.getNextRunTime());
          
          scheduleQueue.reposition(currentIndex, insertionIndex);
        } else if (currentIndex == 0) {
          insertionIndex = 0;
        }
      }
      
      // need to unpark even if the task is not ready, otherwise we may get stuck on an infinite park
      if (insertionIndex == 0) {
        queueListener.handleQueueUpdate();
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
        if (! (tw.task instanceof InternalRunnable)) {
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
   * <p>Abstract implementation for all tasks handled by this pool.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected abstract static class TaskWrapper implements DelayedTask, RunnableContainer {
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
    
    protected OneTimeTaskWrapper(Runnable task, Queue<? extends TaskWrapper> taskQueue, long runTime) {
      super(task);
      
      this.taskQueue = taskQueue;
      this.runTime = runTime;
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
    
    protected RecurringTaskWrapper(Runnable task, QueueSet queueSet, long firstRunTime) {
      super(task);
      
      this.queueSet = queueSet;
      executing = false;
      this.nextRunTime = firstRunTime;
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
        if (queueSet.scheduleQueue.peekFirst() != this) {
          // must be at front of queue to be able to be ran
          return false;
        } else {
          /* we have to reposition to the end atomically so that this task can be removed if 
           * requested to be removed.  We can put it at the end because we know this task wont 
           * run again till it has finished (which it will be inserted at the correct point in 
           * queue then.
           */
          queueSet.scheduleQueue.reposition(0, queueSet.scheduleQueue.size());
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
        updateNextRunTime();
        // now that nextRunTime has been set, resort the queue
        queueSet.reschedule(this);
        
        // only set executing to false AFTER rescheduled
        executing = false;
      }
    }
  }
  
  /**
   * <p>Container for tasks which run with a fixed delay after the previous run.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected static class RecurringDelayTaskWrapper extends RecurringTaskWrapper {
    protected final long recurringDelay;
    
    protected RecurringDelayTaskWrapper(Runnable task, QueueSet queueSet, 
                                        long firstRunTime, long recurringDelay) {
      super(task, queueSet, firstRunTime);
      
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
                                       long firstRunTime, long period) {
      super(task, queueSet, firstRunTime);
      
      this.period = period;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
  }
  
  /**
   * <p>Small interface so we can determine internal tasks which were not submitted by users.  
   * That way they can be filtered out (for example in draining the queue).</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected interface InternalRunnable extends Runnable {
    // nothing added here
  }
}
