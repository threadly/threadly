package org.threadly.test.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.SubmitterSchedulerInterface;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.Clock;
import org.threadly.util.ListUtils;

/**
 * Executor which has no threads itself.  This can be useful for testing.
 * It is similar to {@link TestablePriorityScheduler} except it is much less advanced.
 * It has the same semantics that it only progressed forward with .tick(), but
 * since it is running on the calling thread, calls to .wait() and .sleep() will
 * block (possibly forever).
 * 
 * @author jent - Mike Jensen
 */
public class NoThreadScheduler implements SubmitterSchedulerInterface {
  private final boolean threadSafe;
  private final List<RunnableContainer> taskQueue;
  private long nowInMillis;

  /**
   * Constructs a new thread safe scheduler.
   */
  public NoThreadScheduler() {
    this(true);
  }
  
  /**
   * Constructs a new thread scheduler.  Making scheduler thread safe causes
   * some small additional performance reductions (for when that is important).
   * 
   * @param makeThreadSafe Make scheduler able to accept executions from multiple threads
   */
  public NoThreadScheduler(boolean makeThreadSafe) {
    taskQueue = new LinkedList<RunnableContainer>();
    threadSafe = makeThreadSafe;
    nowInMillis = Clock.accurateTime();
  }
  
  @Override
  public void execute(Runnable task) {
    schedule(task, 0);
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return submitScheduled(task, result, 0);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return submitScheduled(task, 0);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    add(new OneTimeRunnable(task, delayInMs));
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, null, delayInMs);
  }

  @Override
  public <T> Future<T> submitScheduled(Runnable task, T result, long delayInMs) {
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    add(new OneTimeRunnable(lft, delayInMs));
    
    return lft;
  }

  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs) {
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    add(new OneTimeRunnable(lft, delayInMs));
    
    return lft;
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    add(new RecurringRunnable(task, initialDelay, recurringDelay));
  }
  
  private void add(RunnableContainer runnable) {
    if (threadSafe) {
      synchronized (taskQueue) {
        int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, runnable);
        
        taskQueue.add(insertionIndex, runnable);
      }
    } else {
      int insertionIndex = ListUtils.getInsertionEndIndex(taskQueue, runnable);
      
      taskQueue.add(insertionIndex, runnable);
    }
  }
  
  /**
   * Removes a task (recurring or not) if it is waiting in the 
   * queue to be executed.
   * 
   * @param task to remove from execution queue
   * @return true if the task was removed
   */
  public boolean remove(Runnable task) {
    if (threadSafe) {
      synchronized (taskQueue) {
        return removeRunnable(task);
      }
    } else {
      return removeRunnable(task);
    }
  }
  
  private boolean removeRunnable(Runnable task) {
    Iterator<RunnableContainer> it = taskQueue.iterator();
    while (it.hasNext()) {
      if (it.next().runnable.equals(task)) {
        it.remove();
        return true;
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
   * @param currentTime - Time to provide for looking at task run time
   * @return qty of steps taken forward.  Returns zero if no events to run.
   */
  public int tick(long currentTime) {
    if (nowInMillis > currentTime) {
      throw new IllegalArgumentException("Time can not go backwards");
    }
    nowInMillis = currentTime;
    
    int tasks = 0;
    RunnableContainer nextTask = next();
    while (nextTask != null && nextTask.getDelay(TimeUnit.MILLISECONDS) <= 0) {
      tasks++;
      if (threadSafe) {
        synchronized (taskQueue) {
          taskQueue.remove(nextTask); // remove the last peeked item
        }
      } else {
        taskQueue.remove(nextTask); // remove the last peeked item
      }
      
      nextTask.run(currentTime);
      nextTask = next();
    }
    
    return tasks;
  }
  
  private RunnableContainer next() {
    if (threadSafe) {
      synchronized (taskQueue) {
        return taskQueue.isEmpty() ? null : taskQueue.get(0);
      }
    } else {
      return taskQueue.isEmpty() ? null : taskQueue.get(0);
    }
  }
  
  /**
   * Container abstraction to hold runnables for scheduler.
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
    
    public abstract void run(long nowInMs);
  }
  
  /**
   * Runnable container for runnables that only run once
   * with an optional delay.
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
    public void run(long nowInMs) {
      runnable.run();
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(runTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  /**
   * Container for runnables which run multiple times.
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
    public void run(long nowInMs) {
      try {
        runnable.run();
      } finally {
        nextRunTime = nowInMs + recurringDelay;
        add(this);
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - nowInMillis, 
                              TimeUnit.MILLISECONDS);
    }
  }
}
