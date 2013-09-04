package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureVirtualTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;

/**
 * This is a class which is more full featured than {@link TaskExecutorDistributor}, 
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
    this(new PriorityScheduledExecutor(Math.min(expectedParallism, maxThreadCount), 
                                       maxThreadCount, 
                                       DEFAULT_THREAD_KEEPALIVE_TIME, 
                                       TaskPriority.High, 
                                       PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS), 
         new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler) {
    this(DEFAULT_LOCK_PARALISM, scheduler);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler) {
    this(scheduler, new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * Constructor to be used in unit tests.  This allows you to provide a StripedLock 
   * that provides a {@link org.threadly.test.concurrent.lock.TestableLockFactory} so 
   * that this class can be used with the 
   * {@link org.threadly.test.concurrent.TestablePriorityScheduler}.
   * 
   * @param scheduler scheduler to be used for task worker execution 
   * @param sLock lock to be used for controlling access to workers
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, 
                                  StripedLock sLock) {
    super(scheduler, sLock);
    
    this.scheduler = scheduler;
  }
  
  /**
   * Returns a scheduler implementation where all tasks submitted 
   * on this scheduler will run on the provided key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @return scheduler which will only execute based on the provided key
   */
  public SubmitterSchedulerInterface getSubmitterSchedulerForKey(Object threadKey) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    }
    
    return new KeyBasedScheduler(threadKey);
  }

  /**
   * Schedule a one time task with a given delay that will not run concurrently 
   * based off the thread key.
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
    
    if (delayInMs == 0) {
      addTask(threadKey, task);
    } else {
      scheduler.schedule(new AddTask(threadKey, task), 
                         delayInMs);
    }
  }
  
  /**
   * Schedule a recurring task to run.  The recurring delay time will be
   * from the point where execution finished.  This task will not run concurrently 
   * based off the thread key.
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
   * Schedule a task with a given delay.  There is a slight 
   * increase in load when using submitScheduled over schedule.  So 
   * this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param threadKey key which hash will be used to determine which thread to run
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submitScheduledTask(Object threadKey, 
                                                 Runnable task, 
                                                 long delayInMs) {
    return submitScheduledTask(threadKey, task, null, delayInMs);
  }

  /**
   * Schedule a task with a given delay.  There is a slight 
   * increase in load when using submitScheduled over schedule.  So 
   * this should only be used when the future is necessary.
   * 
   * The future .get() method will return null once the runnable has completed.
   * 
   * @param threadKey key which hash will be used to determine which thread to run
   * @param task runnable to execute
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submitScheduledTask(Object threadKey, 
                                                     Runnable task, 
                                                     T result, 
                                                     long delayInMs) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide a threadKey");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureVirtualTask<T>(task, result, 
                                                                        sLock.getLock(threadKey));
    
    if (delayInMs == 0) {
      addTask(threadKey, rf);
    } else {
      scheduler.schedule(new AddTask(threadKey, rf), 
                         delayInMs);
    }
    
    return rf;
  }

  /**
   * Schedule a {@link Callable} with a given delay.  This is 
   * needed when a result needs to be consumed from the 
   * callable.
   * 
   * @param threadKey key which hash will be used to determine which thread to run
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submitScheduledTask(Object threadKey, 
                                                     Callable<T> task, 
                                                     long delayInMs) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide a threadKey");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }

    ListenableRunnableFuture<T> rf = new ListenableFutureVirtualTask<T>(task, sLock.getLock(threadKey));
    
    if (delayInMs == 0) {
      addTask(threadKey, rf);
    } else {
      scheduler.schedule(new AddTask(threadKey, rf), 
                         delayInMs);
    }
    
    return rf;
  }
  
  /**
   * Task which will run delayed to add a task into the queue when ready.
   * 
   * @author jent - Mike Jensen
   */
  protected class AddTask extends VirtualRunnable {
    private final Object key;
    private final Runnable task;
    
    protected AddTask(Object key, Runnable task) {
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
  protected class RecrringTask extends VirtualRunnable {
    private final Object key;
    private final Runnable task;
    private final long recurringDelay;
    
    protected RecrringTask(Object key, Runnable task, long recurringDelay) {
      this.key = key;
      this.task = task;
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    public void run() {
      try {
        if (factory != null && task instanceof VirtualRunnable) {
          ((VirtualRunnable)task).run(factory);
        } else {
          task.run();
        }
      } finally {
        if (! scheduler.isShutdown()) {
          scheduler.schedule(new AddTask(key, this), recurringDelay);
        }
      }
    }
  }
  
  /**
   * Simple simple scheduler implementation that runs all executions and 
   * scheduling on a given key.
   * 
   * @author jent - Mike Jensen
   */
  protected class KeyBasedScheduler extends KeyBasedExecutor 
                                    implements SubmitterSchedulerInterface {
    protected KeyBasedScheduler(Object threadKey) {
      super(threadKey);
    }

    @Override
    public void schedule(Runnable task, long delayInMs) {
      TaskSchedulerDistributor.this.schedule(threadKey, task, 
                                             delayInMs);
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
      TaskSchedulerDistributor.this.scheduleWithFixedDelay(threadKey, task, 
                                                           initialDelay, 
                                                           recurringDelay);
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
    public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
      return submitScheduled(task, null, delayInMs);
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
      return TaskSchedulerDistributor.this.submitScheduledTask(threadKey, task, result, delayInMs);
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
      return TaskSchedulerDistributor.this.submitScheduledTask(threadKey, task, delayInMs);
    }

    @Override
    public boolean isShutdown() {
      return scheduler.isShutdown();
    }
  }
}
