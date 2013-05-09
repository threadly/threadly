package org.threadly.concurrent;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.util.Clock;
import org.threadly.util.ListUtils;

/**
 * Executor which has no threads itself.  This can be useful for testing.
 * It is similar to TestablePriorityScheduler except it is much less advanced.
 * It has the same semantics that it only progressed forward with .tick(), but
 * since it is running on the calling thread, calls to .wait() and .sleep() will
 * block (possibly forever).
 * 
 * @author jent - Mike Jensen
 */
public class NoThreadScheduler implements SimpleSchedulerInterface {
  private final boolean threadSafe;
  private final List<RunnableContainer> taskQueue;

  /**
   * Constructs a new thread safe scheduler
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
  }
  
  @Override
  public void execute(Runnable task) {
    schedule(task, 0);
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    add(new OneTimeRunnable(task, delayInMs));
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
    int tasks = 0;
    RunnableContainer nextTask = next();
    while (nextTask != null && nextTask.getDelay(currentTime, TimeUnit.MILLISECONDS) <= 0) {
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
  
  private abstract class RunnableContainer implements Delayed {
    protected final Runnable runnable;
    
    protected RunnableContainer(Runnable runnable) {
      this.runnable = runnable;
    }
    
    public abstract long getDelay(long nowInMs, TimeUnit timeUnit);
    
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

    @Override
    public long getDelay(TimeUnit unit) {
      return getDelay(Clock.accurateTime(), unit);
    }
  }
  
  private class OneTimeRunnable extends RunnableContainer {
    private final long runTime;
    
    private OneTimeRunnable(Runnable runnable, long delay) {
      super(runnable);
      
      this.runTime = Clock.accurateTime() + delay;
    }
    
    @Override
    public void run(long nowInMs) {
      runnable.run();
    }

    @Override
    public long getDelay(long nowInMs, TimeUnit timeUnit) {
      return timeUnit.convert(runTime - nowInMs, 
                              TimeUnit.MILLISECONDS);
    }
  }
  
  private class RecurringRunnable extends RunnableContainer {
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
    public long getDelay(long nowInMs, TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - nowInMs, 
                              TimeUnit.MILLISECONDS);
    }
  }
}
