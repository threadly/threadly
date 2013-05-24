package org.threadly.concurrent;

import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * This is a class which is more full featured than TaskExecutorDistributor, 
 * but it does require a scheduler implementation in order to be able to perform scheduling.
 * 
 * @author jent - Mike Jensen
 */
public class TaskSchedulerDistributor extends TaskExecutorDistributor {
  private final SimpleSchedulerInterface scheduler;
  
  /**
   * Constructor which creates scheduler based off provided values.  This is a more full 
   * featured task distributor, it allows scheduling and recurring tasks
   * 
   * @param expectedParallism Expected number of keys that will be used in parallel
   * @param maxThreadCount Max thread count (limits the qty of keys which are handled in parallel)
   */
  public TaskSchedulerDistributor(int expectedParallism, int maxThreadCount) {
    this(new PriorityScheduledExecutor(expectedParallism, 
                                       maxThreadCount, 
                                       DEFAULT_THREAD_KEEPALIVE_TIME, 
                                       TaskPriority.High, 
                                       PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT));
  }
  
  /**
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler) {
    this(scheduler, new NativeLock());
  }
  
  /**
   * used for testing, so that agentLock can be held and prevent execution.
   */
  protected TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, VirtualLock agentLock) {
    super(scheduler, agentLock);
    
    this.scheduler = scheduler;
  }

  /**
   * Schedule a task with a given delay.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  public void schedule(Object threadKey, 
                       Runnable task, 
                       long delayInMs) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide a threadKey");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    
    scheduler.schedule(new AddTask(threadKey, task), 
                       delayInMs);
  }
  
  /**
   * Schedule a recurring task to run.  The recurring delay time will be
   * from the point where execution finished.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   */
  public void scheduleWithFixedDelay(Object threadKey, 
                                     Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide a threadKey");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    
    scheduler.schedule(new AddTask(threadKey, 
                                   new RecrringTask(threadKey, task, 
                                                    recurringDelay)), 
                       initialDelay);
  }
  
  /**
   * Task which will run delayed to add a task
   * into the queue when ready.
   * 
   * @author jent - Mike Jensen
   */
  private class AddTask implements Runnable {
    private final Object key;
    private final Runnable task;
    
    private AddTask(Object key, Runnable task) {
      this.key = key;
      this.task = task;
    }

    @Override
    public void run() {
      addTask(key, task);
    }
  }
  
  /**
   * Repeating task container.
   * 
   * @author jent - Mike Jensen
   */
  private class RecrringTask implements Runnable {
    private final Object key;
    private final Runnable task;
    private final long recurringDelay;
    
    private RecrringTask(Object key, Runnable task, long recurringDelay) {
      this.key = key;
      this.task = task;
      this.recurringDelay = recurringDelay;
    }
    @Override
    public void run() {
      try {
        task.run();
      } finally {
        if (! scheduler.isShutdown()) {
          scheduler.schedule(new AddTask(key, this), recurringDelay);
        }
      }
    }
    
  }
}
