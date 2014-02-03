package org.threadly.concurrent;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.Clock;
import org.threadly.util.ListUtils;

/**
 * <p>Executor which has no threads itself.  This allows you to have the same 
 * scheduler abilities (schedule tasks, recurring tasks, etc, etc), without having 
 * to deal with multiple threads, memory barriers, or other similar concerns.  
 * This class can be very useful in GUI development (if you want it to run on the GUI 
 * thread).  It also can be useful in android development in a very similar way.</p>
 * 
 * <p>The tasks in this scheduler are only progressed forward with calls to .tick().  
 * Since it is running on the calling thread, calls to .wait() and .sleep() from sub 
 * tasks will block (possibly forever).  The call to .tick() will not unblock till there 
 * is no more work for the scheduler to currently handle.</p>
 * 
 * @author jent - Mike Jensen
 */
public class NoThreadScheduler implements SubmitterSchedulerInterface {
  protected static final int QUEUE_FRONT_PADDING = 0;
  protected static final int QUEUE_REAR_PADDING = 2;
  
  private final ConcurrentArrayList<RunnableContainer> taskQueue;
  private long nowInMillis;
  
  /**
   * Constructs a new {@link NoThreadScheduler} scheduler.
   */
  public NoThreadScheduler() {
    taskQueue = new ConcurrentArrayList<RunnableContainer>(QUEUE_FRONT_PADDING, 
                                                           QUEUE_REAR_PADDING);
    nowInMillis = Clock.accurateTime();
  }
  
  @Override
  public void execute(Runnable task) {
    schedule(task, 0);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return submitScheduled(task, result, 0);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return submitScheduled(task, 0);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    add(new OneTimeRunnable(task, delayInMs));
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Task can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs can not be negative");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    add(new OneTimeRunnable(lft, delayInMs));
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    if (task == null) {
      throw new IllegalArgumentException("Task can not be null");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs can not be negative");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    add(new OneTimeRunnable(lft, delayInMs));
    
    return lft;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    if (task == null) {
      throw new IllegalArgumentException("Task can not be null");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay can not be negative");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay can not be negative");
    }
    
    add(new RecurringRunnable(task, initialDelay, recurringDelay));
  }
  
  private void add(RunnableContainer runnable) {
    synchronized (taskQueue.getModificationLock()) {
      int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, runnable);
        
      taskQueue.add(insertionIndex, runnable);
    }
  }
  
  /**
   * Removes a task (recurring or not) if it is waiting in the queue to be executed.
   * 
   * @param task to remove from execution queue
   * @return true if the task was removed
   */
  public boolean remove(Runnable task) {
    synchronized (taskQueue) {
      Iterator<RunnableContainer> it = taskQueue.iterator();
      while (it.hasNext()) {
        RunnableContainer rc = it.next();
        if (ContainerHelper.isContained(rc.runnable, task)) {
          it.remove();
          return true;
        }
      }
    }
    
    return false;
  }
  
  /**
   * Removes a Callable task if it is waiting in the queue to be executed.
   * 
   * @param task to remove from execution queue
   * @return true if the task was removed
   */
  public boolean remove(Callable<?> task) {
    synchronized (taskQueue) {
      Iterator<RunnableContainer> it = taskQueue.iterator();
      while (it.hasNext()) {
        RunnableContainer rc = it.next();
        if (ContainerHelper.isContained(rc.runnable, task)) {
          it.remove();
          return true;
        }
      }
    }
    
    return false;
  }

  @Override
  public boolean isShutdown() {
    return false;
  }
  
  /**
   * Progresses tasks for the current time.  This will block as it runs
   * as many scheduled or waiting tasks as possible.
   * 
   * @return qty of steps taken forward.  Returns zero if no events to run.
   */
  public int tick() {
    return tick(Clock.accurateTime());
  }
  
  /**
   * This progresses tasks based off the time provided.  This is primarily
   * used in testing by providing a possible time in the future (to execute future tasks).
   * 
   * @param currentTime Time to provide for looking at task run time
   * @return qty of tasks run in this tick call.
   */
  public int tick(long currentTime) {
    if (nowInMillis > currentTime) {
      throw new IllegalArgumentException("Time can not go backwards");
    }
    nowInMillis = currentTime;
    
    int tasks = 0;
    RunnableContainer nextTask;
    while ((nextTask = next()) != null && 
           nextTask.getDelay(TimeUnit.MILLISECONDS) <= 0) {
      tasks++;
      
      // call will remove task from queue, or reposition as necessary
      nextTask.run(currentTime);
    }
    
    return tasks;
  }
  
  private RunnableContainer next() {
    synchronized (taskQueue) {
      return taskQueue.isEmpty() ? null : taskQueue.getFirst();
    }
  }
  
  /**
   * <p>Container abstraction to hold runnables for scheduler.</p>
   * 
   * @author jent - Mike Jensen
   */
  private abstract class RunnableContainer implements Delayed {
    protected final Runnable runnable;
    
    protected RunnableContainer(Runnable runnable) {
      this.runnable = runnable;
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
    
    protected void run(long nowInMs) {
      prepareForRun(nowInMs);
      
      runnable.run();
    }
    
    protected abstract void prepareForRun(long nowInMs);
  }
  
  /**
   * <p>Runnable container for runnables that only run once
   * with an optional delay.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class OneTimeRunnable extends RunnableContainer {
    private final long runTime;
    
    public OneTimeRunnable(Runnable runnable, long delay) {
      super(runnable);
      
      this.runTime = nowInMillis + delay;
    }
    
    @Override
    protected void prepareForRun(long nowInMs) {
      synchronized (taskQueue.getModificationLock()) {
        taskQueue.remove(this);
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(runTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * <p>Container for runnables which run multiple times.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class RecurringRunnable extends RunnableContainer {
    private final long recurringDelay;
    private long nextRunTime;
    
    public RecurringRunnable(Runnable runnable, long initialDelay, long recurringDelay) {
      super(runnable);
      
      this.recurringDelay = recurringDelay;
      nextRunTime = Clock.accurateTime() + initialDelay;
    }
    
    @Override
    public void prepareForRun(long nowInMs) {
      synchronized (taskQueue.getModificationLock()) {
        int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, recurringDelay, 
                                                            true);
        
        /* provide the option to search backwards since the item 
         * will most likely be towards the back of the queue */
        taskQueue.reposition(this, insertionIndex, false);
      }
      
      nextRunTime = nowInMs + recurringDelay;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
}
