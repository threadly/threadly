package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.StripedLock;

/**
 * <p>This is a class which is more full featured than {@link TaskExecutorDistributor}, 
 * but it does require a scheduler implementation in order to be able to perform scheduling.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
public class TaskSchedulerDistributor extends TaskExecutorDistributor {
  private final SimpleSchedulerInterface scheduler;

  /**
   * Constructor which creates scheduler based off provided values.  This is a more full 
   * featured task distributor, it allows scheduling and recurring tasks.
   * 
   * @deprecated Use a constructor that takes a SimpleSchedulerInterface, this will be removed in 2.0.0
   * 
   * @param expectedParallism Expected number of keys that will be used in parallel
   * @param maxThreadCount Max thread count (limits the qty of keys which are handled in parallel)
   */
  @Deprecated
  public TaskSchedulerDistributor(int expectedParallism, int maxThreadCount) {
    this(expectedParallism, maxThreadCount, Integer.MAX_VALUE);
  }
  
  /**
   * Constructor which creates scheduler based off provided values.  This is a more full 
   * featured task distributor, it allows scheduling and recurring tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * @deprecated Use a constructor that takes a SimpleSchedulerInterface, this will be removed in 2.0.0
   * 
   * @param expectedParallism Expected number of keys that will be used in parallel
   * @param maxThreadCount Max thread count (limits the qty of keys which are handled in parallel)
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  @Deprecated
  public TaskSchedulerDistributor(int expectedParallism, int maxThreadCount, int maxTasksPerCycle) {
    this(new PriorityScheduledExecutor(Math.min(expectedParallism, maxThreadCount), 
                                       maxThreadCount, 
                                       DEFAULT_THREAD_KEEPALIVE_TIME, 
                                       TaskPriority.High, 
                                       PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS), 
         new StripedLock(expectedParallism), maxTasksPerCycle, false);
  }
  
  /**
   * Constructor to use a provided scheduler implementation for running tasks.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the "getTaskQueueSize" call (thus preferring 
   * high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler) {
    this(DEFAULT_LOCK_PARALISM, scheduler, Integer.MAX_VALUE, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.  
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.  
   * 
   * This constructs with a default expected level of concurrency of 16.
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, boolean accurateQueueSize) {
    this(DEFAULT_LOCK_PARALISM, scheduler, Integer.MAX_VALUE, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.  
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for 
   * given thread keys.  There is a performance hit associated with this, so this should 
   * only be enabled if "getTaskQueueSize" calls will be used.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the "getTaskQueueSize" call (thus preferring 
   * high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, int maxTasksPerCycle) {
    this(DEFAULT_LOCK_PARALISM, scheduler, maxTasksPerCycle, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.  
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be 
   * enabled if "getTaskQueueSize" calls will be used.
   * 
   * This constructs with a default expected level of concurrency of 16. 
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, int maxTasksPerCycle, 
                                  boolean accurateQueueSize) {
    this(DEFAULT_LOCK_PARALISM, scheduler, maxTasksPerCycle, accurateQueueSize);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * "getTaskQueueSize" call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler) {
    this(expectedParallism, scheduler, Integer.MAX_VALUE, false);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel. 
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  boolean accurateQueueSize) {
    this(expectedParallism, scheduler, Integer.MAX_VALUE);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * "getTaskQueueSize" call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  int maxTasksPerCycle) {
    this(expectedParallism, scheduler, maxTasksPerCycle, false);
  }
    
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * This also allows you to specify if you want accurate queue sizes to be 
   * tracked for given thread keys.  There is a performance hit associated with this, 
   * so this should only be enabled if "getTaskQueueSize" calls will be used.
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  
   *                  Ideally has as many possible threads as keys that 
   *                  will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize true to make "getTaskQueueSize" more accurate
   */
  public TaskSchedulerDistributor(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                  int maxTasksPerCycle, boolean accurateQueueSize) {
    this(scheduler, new StripedLock(expectedParallism), 
         maxTasksPerCycle, accurateQueueSize);
  }
  
  /**
   * Constructor to be used in unit tests.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it 
   * yields to another key.  This can make it more fair, and make it so no single key can 
   * starve other keys from running.  The lower this is set however, the less efficient it 
   * becomes in part because it has to give up the thread and get it again, but also because 
   * it must copy the subset of the task queue which it can run.
   * 
   * @param scheduler scheduler to be used for task worker execution 
   * @param sLock lock to be used for controlling access to workers
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  protected TaskSchedulerDistributor(SimpleSchedulerInterface scheduler, StripedLock sLock, 
                                     int maxTasksPerCycle, boolean accurateQueueSize) {
    super(scheduler, sLock, maxTasksPerCycle, accurateQueueSize);
    
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
   * @deprecated use scheduleTask, this will be removed in 2.0.0
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  @Deprecated
  public void schedule(Object threadKey, 
                       Runnable task, 
                       long delayInMs) {
    scheduleTask(threadKey, task, delayInMs);
  }

  /**
   * Schedule a one time task with a given delay that will not run concurrently 
   * based off the thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  public void scheduleTask(Object threadKey, 
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
      addTask(threadKey, task, executor);
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
   * @deprecated use scheduleTaskWithFixedDelay, this will be removed in 2.0.0
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   */
  @Deprecated
  public void scheduleWithFixedDelay(Object threadKey, 
                                     Runnable task, 
                                     long initialDelay, 
                                     long recurringDelay) {
    scheduleTaskWithFixedDelay(threadKey, task, initialDelay, recurringDelay);
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
  public void scheduleTaskWithFixedDelay(Object threadKey, 
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

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task, result);
    
    if (delayInMs == 0) {
      addTask(threadKey, rf, executor);
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

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task);
    
    if (delayInMs == 0) {
      addTask(threadKey, rf, executor);
    } else {
      scheduler.schedule(new AddTask(threadKey, rf), 
                         delayInMs);
    }
    
    return rf;
  }
  
  /**
   * <p>Task which will run delayed to add a task into the queue when ready.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class AddTask implements Runnable, 
                                     RunnableContainerInterface {
    private final Object key;
    private final Runnable task;
    
    protected AddTask(Object key, Runnable task) {
      this.key = key;
      this.task = task;
    }

    @Override
    public void run() {
      addTask(key, task, SameThreadSubmitterExecutor.instance());
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Repeating task container.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class RecrringTask implements Runnable, 
                                          RunnableContainerInterface {
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
        task.run();
      } finally {
        if (! scheduler.isShutdown()) {
          scheduler.schedule(new AddTask(key, this), recurringDelay);
        }
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Simple simple scheduler implementation that runs all 
   * executions and scheduling on a given key.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class KeyBasedScheduler extends KeyBasedSubmitter 
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
