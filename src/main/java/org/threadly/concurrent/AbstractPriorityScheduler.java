package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.SortUtils;

/**
 * Abstract implementation for implementations of {@link PrioritySchedulerService}.  In general 
 * this wont be useful outside of Threadly developers, but must be a public interface since it is 
 * used in sub-packages.
 * <p>
 * If you do find yourself using this class, please post an issue on github to tell us why.  If 
 * there is something you want our schedulers to provide, we are happy to hear about it.
 * 
 * @since 4.3.0
 */
public abstract class AbstractPriorityScheduler extends AbstractSubmitterScheduler 
                                                implements PrioritySchedulerService {
  protected static final TaskPriority DEFAULT_PRIORITY = TaskPriority.High;
  protected static final int DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS = 500;
  // tuned for performance of scheduled tasks
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;

  static {
    @SuppressWarnings("unused") // https://bugs.openjdk.java.net/browse/JDK-8074773
    Class<?> ensureLoaded = LockSupport.class;
  }

  protected final TaskPriority defaultPriority;
  
  protected AbstractPriorityScheduler(TaskPriority defaultPriority) {
    if (defaultPriority == null) {
      this.defaultPriority = DEFAULT_PRIORITY;
    } else {
      this.defaultPriority = defaultPriority;
    }
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
    getQueueManager().setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
  }
  
  @Override
  public long getMaxWaitForLowPriority() {
    return getQueueManager().getMaxWaitForLowPriority();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return defaultPriority;
  }

  @Override
  protected final void doSchedule(Runnable task, long delayInMillis) {
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
                                                   long delayInMillis, TaskPriority priority);

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    schedule(task, 0, priority);
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
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, 
                                                 long delayInMs, TaskPriority priority) {
    return submitScheduled(RunnableCallableAdapter.adapt(task, result), delayInMs, priority);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority) {
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    if (priority == null) {
      priority = defaultPriority;
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<>(task, this);
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

  @Override
  public boolean remove(Runnable task) {
    return getQueueManager().remove(task);
  }
  
  @Override
  public boolean remove(Callable<?> task) {
    return getQueueManager().remove(task);
  }

  /**
   * Call to get reference to {@link QueueManager}.  This reference can be used to get access to 
   * queues directly, or perform operations which are distributed to multiple queues.  This 
   * reference can not be maintained in this abstract class to allow the potential for 
   * {@link QueueSetListener}'s which want to reference things in {@code this}.
   * 
   * @return Manager for queue sets
   */
  protected abstract QueueManager getQueueManager();
  
  @Override
  public int getQueuedTaskCount() {
    int result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += getQueueManager().getQueueSet(p).queueSize();
    }
    return result;
  }
  
  @Override
  public int getQueuedTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getQueuedTaskCount();
    }
    
    return getQueueManager().getQueueSet(priority).queueSize();
  }
  
  @Override
  public int getWaitingForExecutionTaskCount() {
    int result = 0;
    for (TaskPriority p : TaskPriority.values()) {
      result += getWaitingForExecutionTaskCount(p);
    }
    return result;
  }
  
  @Override
  public int getWaitingForExecutionTaskCount(TaskPriority priority) {
    if (priority == null) {
      return getWaitingForExecutionTaskCount();
    }
    
    QueueSet qs = getQueueManager().getQueueSet(priority);
    int result = qs.executeQueue.size();
    for (int i = 0; i < qs.scheduleQueue.size(); i++) {
      try {
        if (qs.scheduleQueue.get(i).getScheduleDelay() > 0) {
          break;
        } else {
          result++;
        }
      } catch (IndexOutOfBoundsException e) {
        break;
      }
    }
    return result;
  }
  
  /**
   * Interface to be notified when relevant changes happen to the queue set.
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
   * Class to contain structures for both execution and scheduling.  It also contains logic for 
   * how we get and add tasks to this queue.
   * <p>
   * This allows us to have one structure for each priority.  Each structure determines what is 
   * the next task for a given priority.
   * 
   * @since 4.0.0
   */
  protected static class QueueSet {
    protected final QueueSetListener queueListener;
    protected final ConcurrentLinkedQueue<OneTimeTaskWrapper> executeQueue;
    protected final ConcurrentArrayList<TaskWrapper> scheduleQueue;
    protected final Function<Integer, Long> scheduleQueueRunTimeByIndex;
    
    public QueueSet(QueueSetListener queueListener) {
      this.queueListener = queueListener;
      this.executeQueue = new ConcurrentLinkedQueue<>();
      this.scheduleQueue = new ConcurrentArrayList<>(QUEUE_FRONT_PADDING, QUEUE_REAR_PADDING);
      scheduleQueueRunTimeByIndex = (index) -> scheduleQueue.get(index).getRunTime();
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
        insertionIndex = SortUtils.getInsertionEndIndex(scheduleQueueRunTimeByIndex, 
                                                        scheduleQueue.size() - 1, 
                                                        task.getRunTime(), true);
        scheduleQueue.add(insertionIndex, task);
      }
      
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
            tw.invalidate();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.task, task)) {
            tw.invalidate();
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
            tw.invalidate();
            return true;
          }
        }
      }
      synchronized (scheduleQueue.getModificationLock()) {
        Iterator<? extends TaskWrapper> it = scheduleQueue.iterator();
        while (it.hasNext()) {
          TaskWrapper tw = it.next();
          if (ContainerHelper.isContained(tw.task, task)) {
            tw.invalidate();
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
  
    private static void clearQueue(Collection<? extends TaskWrapper> queue, 
                                   List<TaskWrapper> resultList) {
      boolean resultWasEmpty = resultList.isEmpty();
      Iterator<? extends TaskWrapper> it = queue.iterator();
      while (it.hasNext()) {
        TaskWrapper tw = it.next();
        // no need to cancel and return tasks which are already canceled
        if (! (tw.task instanceof Future) || ! ((Future<?>)tw.task).isCancelled()) {
          tw.invalidate();
          // don't return tasks which were used only for internal behavior management
          if (! (tw.task instanceof InternalRunnable)) {
            if (resultWasEmpty) {
              resultList.add(tw);
            } else {
              resultList.add(SortUtils.getInsertionEndIndex((index) -> 
                                                              resultList.get(index).getRunTime(), 
                                                            resultList.size() - 1, 
                                                            tw.getRunTime(), true), 
                             tw);
            }
          }
        }
      }
      queue.clear();
    }
    
    /**
     * Gets the next task from this {@link QueueSet}.  This inspects both the execute queue and 
     * against scheduled tasks to determine which task in this {@link QueueSet} should be executed 
     * next.
     * <p>
     * The task returned from this may not be ready to executed, but at the time of calling it 
     * will be the next one to execute.
     * 
     * @return TaskWrapper which will be executed next, or {@code null} if there are no tasks
     */
    public TaskWrapper getNextTask() {
      TaskWrapper scheduledTask = scheduleQueue.peekFirst();
      TaskWrapper executeTask = executeQueue.peek();
      if (executeTask != null) {
        if (scheduledTask != null && scheduledTask.getRunTime() < executeTask.getRunTime()) {
          return scheduledTask;
        } else {
          return executeTask;
        }
      } else {
        return scheduledTask;
      }
    }
  }
  
  /**
   * A service which manages the execute queues.  It runs a task to consume from the queues and 
   * execute those tasks as workers become available.  It also manages the queues as tasks are 
   * added, removed, or rescheduled.
   * 
   * @since 3.4.0
   */
  protected static class QueueManager {
    protected final QueueSet highPriorityQueueSet;
    protected final QueueSet lowPriorityQueueSet;
    protected final QueueSet starvablePriorityQueueSet;
    private volatile long maxLowPriorityWaitMillis;
    
    public QueueManager(QueueSetListener queueSetListener, long maxWaitForLowPriorityInMs) {
      this.highPriorityQueueSet = new QueueSet(queueSetListener);
      this.lowPriorityQueueSet = new QueueSet(queueSetListener);
      this.starvablePriorityQueueSet = new QueueSet(queueSetListener);
      
      // call to verify and set values
      setMaxWaitForLowPriority(maxWaitForLowPriorityInMs);
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
     * Removes any tasks waiting to be run.  Will not interrupt any tasks currently running.  But 
     * will avoid additional tasks from being run (unless they are allowed to be added during or 
     * after this call).  
     * <p>
     * If tasks are added concurrently during this invocation they may or may not be removed.
     * 
     * @return List of runnables which were waiting in the task queue to be executed (and were now removed)
     */
    public List<Runnable> clearQueue() {
      List<TaskWrapper> wrapperList = new ArrayList<>(highPriorityQueueSet.queueSize() + 
                                                        lowPriorityQueueSet.queueSize() + 
                                                        starvablePriorityQueueSet.queueSize());
      highPriorityQueueSet.drainQueueInto(wrapperList);
      lowPriorityQueueSet.drainQueueInto(wrapperList);
      starvablePriorityQueueSet.drainQueueInto(wrapperList);
      
      return ContainerHelper.getContainedRunnables(wrapperList);
    }
    
    /**
     * Gets the next task currently queued for execution.  This task may be ready to execute, or 
     * just queued.  If a queue update comes in, this must be re-invoked to see what task is now 
     * next.  If there are no tasks ready to be executed this will simply return {@code null}.
     * 
     * @param allowStarvable {@code true} to return starvable tasks if no other task is available and ready
     * @return Task to be executed next, or {@code null} if no tasks at all are queued
     */
    public TaskWrapper getNextTask(boolean allowStarvable) {
      // First compare between high and low priority task queues
      // then depending on that state, we may check starvable
      TaskWrapper nextTask;
      TaskWrapper nextHighTask = highPriorityQueueSet.getNextTask();
      TaskWrapper nextLowTask = lowPriorityQueueSet.getNextTask();
      if (nextLowTask == null) {
        nextTask = nextHighTask;
      } else if (nextHighTask == null) {
        nextTask = nextLowTask;
      } else if (nextHighTask.getRunTime() <= nextLowTask.getRunTime()) {
        nextTask = nextHighTask;
      } else if (nextLowTask.getPureRunTime() + maxLowPriorityWaitMillis < nextHighTask.getPureRunTime() ||
          // before the above check we know the low priority has been waiting longer than the high 
          // priority.  If the low priority has been waiting longer than the timeout, it can now 
          // be returned as the next ready task
          //
          // OR
          //
          // If the high priority task is not ready to execute anyways, then we will provide the 
          // low.  The low priority will either be ready to run, or will be ready to run sooner.
          nextHighTask.getScheduleDelay() > 0) {
        nextTask = nextLowTask;
      } else {
        // task is ready to run, low priority is also ready, but has not been waiting long enough
        nextTask = nextHighTask;
      }
      
      if (! allowStarvable) {
        return nextTask;
      } else if (nextTask == null) {
        return starvablePriorityQueueSet.getNextTask();
      } else if (nextTask.getScheduleDelay() > 0) {
        TaskWrapper nextStarvableTask = starvablePriorityQueueSet.getNextTask();
        if (nextStarvableTask != null && 
            nextStarvableTask.getPureRunTime() < nextTask.getPureRunTime()) {
          return nextStarvableTask;
        } else {
          return nextTask;
        }
      } else {
        return nextTask;
      }
    }
    
    /**
     * Removes the runnable task from the execution queue.  It is possible for the runnable to 
     * still run until this call has returned.
     * <p>
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
     * <p>
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
      
      this.maxLowPriorityWaitMillis = maxWaitForLowPriorityInMs;
    }
    
    /**
     * Getter for the amount of time a low priority task will wait during thread contention before 
     * it is eligible for execution.
     * 
     * @return currently set max wait for low priority task
     */
    public long getMaxWaitForLowPriority() {
      return maxLowPriorityWaitMillis;
    }
  }
  
  /**
   * Abstract implementation for all tasks handled by this pool.
   * 
   * @since 1.0.0
   */
  protected abstract static class TaskWrapper implements RunnableContainer {
    protected final Runnable task;
    protected volatile boolean invalidated;
    
    public TaskWrapper(Runnable task) {
      this.task = task;
      invalidated = false;
    }
    
    /**
     * Similar to {@link Runnable#run()}, this is invoked to execute the contained task.  One 
     * critical difference is this implementation should never throw an exception (even 
     * {@link RuntimeException}'s).  Throwing such an exception would result in the worker thread 
     * dying (and being leaked from the pool).
     */
    public abstract void runTask();

    /**
     * Attempts to invalidate the task from running (assuming it has not started yet).  If the 
     * task is recurring then future executions will also be avoided.
     */
    public void invalidate() {
      invalidated = true;
    }
    
    /**
     * Get an execution reference so that we can ensure thread safe access into 
     * {@link #canExecute(short)}.
     * 
     * @return Short to identify execution state 
     */
    public abstract short getExecuteReference();
    
    /**
     * Called as the task is being removed from the queue to prepare for execution.  The reference 
     * provided here should be captured from {@link #getExecuteReference()}.
     * 
     * @param executeReference Reference checked to ensure thread safe task execution
     * @return true if the task should be executed
     */
    public abstract boolean canExecute(short executeReference);
    
    /**
     * Get the absolute time when this should run, in comparison with the time returned from 
     * {@link org.threadly.util.Clock#accurateForwardProgressingMillis()}.
     * 
     * @return Absolute time in millis this task should run
     */
    public abstract long getRunTime();
    
    /**
     * Simple getter for the run time, this is expected to do NO operations for calculating the 
     * run time.  The main reason this is used over {@link #getRunTime()} is to allow the JVM to 
     * jit the function better.  Because of the nature of this, this can only be used at very 
     * specific points in the tasks lifecycle, and can not be used for sorting operations.
     * 
     * @return An un-molested representation of the stored absolute run time
     */
    public abstract long getPureRunTime();
    
    /**
     * Call to see how long the task should be delayed before execution.  While this may return 
     * either positive or negative numbers, only an accurate number is returned if the task must 
     * be delayed for execution.  If the task is ready to execute it may return zero even though 
     * it is past due.  For that reason you can NOT use this to compare two tasks for execution 
     * order, instead you should use {@link #getRunTime()}.
     * 
     * @return delay in milliseconds till task can be run
     */
    public abstract long getScheduleDelay();
    
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
   * Wrapper for tasks which only executes once.
   * 
   * @since 1.0.0
   */
  protected abstract static class OneTimeTaskWrapper extends TaskWrapper {
    protected final Queue<? extends TaskWrapper> taskQueue;
    protected final long runTime;
    // optimization to avoid queue traversal on failure to remove, cheaper than AtomicBoolean
    protected volatile boolean executed;  // default false
    
    public OneTimeTaskWrapper(Runnable task, Queue<? extends TaskWrapper> taskQueue, long runTime) {
      super(task);
      
      this.taskQueue = taskQueue;
      this.runTime = runTime;
    }
    
    @Override
    public long getPureRunTime() {
      return runTime;
    }
    
    @Override
    public long getRunTime() {
      return runTime;
    }

    @Override
    public void runTask() {
      if (! invalidated) {
        try {
          task.run();
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        }
      }
    }
    
    @Override
    public short getExecuteReference() {
      // we ignore the reference since one time tasks are deterministically removed from the queue
      return 0;
    }

    @Override
    public boolean canExecute(short ignoredExecuteReference) {
      if (! executed && 
          (executed = true) & // set executed as soon as possible, before removal attempt
          taskQueue.remove(this)) { // every task is wrapped in a unique wrapper, so we can remove 'this' safely
        return true;
      } else {
        return false;
      }
    }
  }
  
  /**
   * Implementation of {@link OneTimeTaskWrapper} that provides an accurate schedule delay.
   * 
   * @since 5.41
   */
  protected static class AccurateOneTimeTaskWrapper extends OneTimeTaskWrapper {
    public AccurateOneTimeTaskWrapper(Runnable task, Queue<? extends TaskWrapper> taskQueue,
                                      long runTime) {
      super(task, taskQueue, runTime);
    }

    @Override
    public long getScheduleDelay() {
      if (runTime > Clock.lastKnownForwardProgressingMillis()) {
        return runTime - Clock.accurateForwardProgressingMillis();
      } else {
        return 0;
      }
    }
  }
  
  /**
   * Implementation of {@link OneTimeTaskWrapper} that provides a schedule delay based off  
   * {@link Clock#lastKnownForwardProgressingMillis()}.
   * 
   * @since 5.41
   */
  protected static class GuessOneTimeTaskWrapper extends OneTimeTaskWrapper {
    public GuessOneTimeTaskWrapper(Runnable task, Queue<? extends TaskWrapper> taskQueue,
                                   long runTime) {
      super(task, taskQueue, runTime);
    }

    @Override
    public long getScheduleDelay() {
      return runTime - Clock.lastKnownForwardProgressingMillis();
    }
  }
  
  /**
   * Similar to {@link OneTimeTaskWrapper} except that this task must always be eligible for 
   * execution immediately.  This allows for some minor assumptions to be made to facilitate 
   * performance.
   * 
   * @since 5.26
   */
  protected static class ImmediateTaskWrapper extends OneTimeTaskWrapper {
    public ImmediateTaskWrapper(Runnable task, Queue<? extends TaskWrapper> taskQueue) {
      super(task, taskQueue, Clock.lastKnownForwardProgressingMillis());
    }
    
    @Override
    public long getScheduleDelay() {
      // override to avoid volatile read performance hit
      return 0;
    }
  }
  
  /**
   * Abstract wrapper for any tasks which run repeatedly.
   * 
   * @since 3.1.0
   */
  protected abstract static class RecurringTaskWrapper extends TaskWrapper {
    protected final QueueSet queueSet;
    protected volatile boolean executing; // improves performance compared to executeFlipCounter % 2 == 1
    protected long nextRunTime;
    // executeFlipCounter is used to prevent multiple executions when consumed concurrently
    // only changed when queue is locked...overflow is fine
    private volatile short executeFlipCounter;
    
    public RecurringTaskWrapper(Runnable task, QueueSet queueSet, long firstRunTime) {
      super(task);
      
      this.queueSet = queueSet;
      executing = false;
      this.nextRunTime = firstRunTime;
      executeFlipCounter = 0;
    }
    
    @Override
    public long getPureRunTime() {
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
    public short getExecuteReference() {
      return executeFlipCounter;
    }

    @Override
    public boolean canExecute(short executeReference) {
      if (executing | executeFlipCounter != executeReference) {
        return false;
      }
      synchronized (queueSet.scheduleQueue.getModificationLock()) {
        if (executing | executeFlipCounter != executeReference) {
          // this task is already running, or not ready to run, so ignore
          return false;
        } else {
          /* we have to reposition to the end atomically so that this task can be removed if 
           * requested to be removed.  We can put it at the end because we know this task wont 
           * run again till it has finished (which it will be inserted at the correct point in 
           * queue then.
           */
          int sourceIndex = queueSet.scheduleQueue.indexOf(this);
          if (sourceIndex >= 0) {
            if (sourceIndex < queueSet.scheduleQueue.size() - 1 && 
                queueSet.scheduleQueue.get(sourceIndex + 1).getRunTime() != Long.MAX_VALUE) {
              queueSet.scheduleQueue.reposition(sourceIndex, queueSet.scheduleQueue.size());
            }
            executing = true;
            executeFlipCounter++;
            return true;
          } else {
            return false;
          }
        }
      }
    }

    /**
     * Call to find and reposition a scheduled task.  It is expected that the task provided has 
     * already been added to the queue.  This call will use 
     * {@link RecurringTaskWrapper#getRunTime()} to figure out what the new position within the 
     * queue should be.
     */
    protected void reschedule() {
      int insertionIndex = -1;
      synchronized (queueSet.scheduleQueue.getModificationLock()) {
        int currentIndex = queueSet.scheduleQueue.lastIndexOf(this);
        if (currentIndex > 0) {
          insertionIndex = SortUtils.getInsertionEndIndex(queueSet.scheduleQueueRunTimeByIndex, 
                                                          queueSet.scheduleQueue.size() - 1, 
                                                          nextRunTime, true);
          
          queueSet.scheduleQueue.reposition(currentIndex, insertionIndex);
        } else if (currentIndex == 0) {
          insertionIndex = 0;
        } else {
          // task removed, no-op, but might as well tidy up the state even though nothing cares
        }
        
        // we can only update executing AFTER the reposition has finished
        // The synchronization lock must be held during this because changing executing
        // changes the scheduled delay, and thus we can not have other threads examining the task queue
        executing = false;
        executeFlipCounter++;  // increment again to indicate execute state change
      }

      // kind of awkward we need to know here, but we we need to let the queue set know if the head changed
      if (insertionIndex == 0) {
        queueSet.queueListener.handleQueueUpdate();
      }
    }
    
    /**
     * Called when the implementing class should update the variable {@code nextRunTime} to be the 
     * next absolute time in milliseconds the task should run.
     */
    protected abstract void updateNextRunTime();

    @Override
    public void runTask() {
      if (invalidated) {
        return;
      }
      
      try {
        task.run();
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
      }
      
      if (! invalidated) {
        updateNextRunTime();
        // now that nextRunTime has been set, resort the queue (ask reschedule)
        reschedule();  // this will set executing to false atomically with the resort
      }
    }
  }
  
  /**
   * Container for tasks which run with a fixed delay after the previous run.  This implementation 
   * will provide an accurate {@link #getScheduleDelay()}.
   * 
   * @since 5.41
   */
  protected static class AccurateRecurringDelayTaskWrapper extends RecurringTaskWrapper {
    protected final long recurringDelay;
    
    public AccurateRecurringDelayTaskWrapper(Runnable task, QueueSet queueSet, 
                                             long firstRunTime, long recurringDelay) {
      super(task, queueSet, firstRunTime);
      
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime = Clock.accurateForwardProgressingMillis() + recurringDelay;
    }
    
    @Override
    public long getScheduleDelay() {
      if (executing) {
        // this would only be likely if two threads were trying to run the same task
        return Long.MAX_VALUE;
      } else if (nextRunTime > Clock.lastKnownForwardProgressingMillis()) {
        return nextRunTime - Clock.accurateForwardProgressingMillis();
      } else {
        return 0;
      }
    }
  }
  
  /**
   * Container for tasks which run with a fixed delay after the previous run.  This implementation 
   * provides a schedule delay based off {@link Clock#lastKnownForwardProgressingMillis()}.
   * 
   * @since 5.41
   */
  protected static class GuessRecurringDelayTaskWrapper extends RecurringTaskWrapper {
    protected final long recurringDelay;
    
    public GuessRecurringDelayTaskWrapper(Runnable task, QueueSet queueSet, 
                                          long firstRunTime, long recurringDelay) {
      super(task, queueSet, firstRunTime);
      
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime = Clock.accurateForwardProgressingMillis() + recurringDelay;
    }
    
    @Override
    public long getScheduleDelay() {
      if (executing) {
        // this would only be likely if two threads were trying to run the same task
        return Long.MAX_VALUE;
      } else {
        return nextRunTime - Clock.lastKnownForwardProgressingMillis();
      }
    }
  }
  
  /**
   * Wrapper for tasks which run at a fixed period (regardless of execution duration).  This 
   * implementation will provide an accurate {@link #getScheduleDelay()}.
   * 
   * @since 5.41
   */
  protected static class AccurateRecurringRateTaskWrapper extends RecurringTaskWrapper {
    protected final long period;
    
    public AccurateRecurringRateTaskWrapper(Runnable task, QueueSet queueSet, 
                                            long firstRunTime, long period) {
      super(task, queueSet, firstRunTime);
      
      this.period = period;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
    
    @Override
    public long getScheduleDelay() {
      if (executing) {
        // this would only be likely if two threads were trying to run the same task
        return Long.MAX_VALUE;
      } else if (nextRunTime > Clock.lastKnownForwardProgressingMillis()) {
        return nextRunTime - Clock.accurateForwardProgressingMillis();
      } else {
        return 0;
      }
    }
  }
  
  /**
   * Wrapper for tasks which run at a fixed period (regardless of execution time).  This 
   * implementation provides a schedule delay based off 
   * {@link Clock#lastKnownForwardProgressingMillis()}.
   * 
   * @since 5.41
   */
  protected static class GuessRecurringRateTaskWrapper extends RecurringTaskWrapper {
    protected final long period;
    
    public GuessRecurringRateTaskWrapper(Runnable task, QueueSet queueSet, 
                                         long firstRunTime, long period) {
      super(task, queueSet, firstRunTime);
      
      this.period = period;
    }
    
    @Override
    protected void updateNextRunTime() {
      nextRunTime += period;
    }
    
    @Override
    public long getScheduleDelay() {
      if (executing) {
        // this would only be likely if two threads were trying to run the same task
        return Long.MAX_VALUE;
      } else {
        return nextRunTime - Clock.lastKnownForwardProgressingMillis();
      }
    }
  }
  
  /**
   * Small interface so we can determine internal tasks which were not submitted by users.  That 
   * way they can be filtered out (for example in draining the queue).
   * 
   * @since 4.3.0
   */
  protected interface InternalRunnable extends Runnable {
    // nothing added here
  }
}
