package org.threadly.concurrent;

import java.util.concurrent.Callable;

import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>This is a class which is more full featured than {@link KeyDistributedExecutor}, but it does 
 * require a scheduler implementation in order to be able to perform scheduling.</p>
 * 
 * <p>The same guarantees and restrictions for the {@link KeyDistributedExecutor} also exist for 
 * this class.  Please read the javadoc for {@link KeyDistributedExecutor} to understand more 
 * about how this operates.</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.5.0 (existed since 1.0.0 as TaskSchedulerDistributor)
 */
public class KeyDistributedScheduler extends KeyDistributedExecutor {
  private final SimpleSchedulerInterface scheduler;

  /**
   * Constructor to use a provided scheduler implementation for running tasks.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the {@link #getTaskQueueSize(Object)} call (thus 
   * preferring high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   */
  public KeyDistributedScheduler(SimpleSchedulerInterface scheduler) {
    this(DEFAULT_LOCK_PARALISM, scheduler, Integer.MAX_VALUE, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.  
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be tracked for 
   * given thread keys.  There is a performance hit associated with this, so this should only be 
   * enabled if {@link #getTaskQueueSize(Object)} calls will be used.  
   * 
   * This constructs with a default expected level of concurrency of 16.
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedScheduler(SimpleSchedulerInterface scheduler, boolean accurateQueueSize) {
    this(DEFAULT_LOCK_PARALISM, scheduler, Integer.MAX_VALUE, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.  
   * 
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the {@link #getTaskQueueSize(Object)} call (thus 
   * preferring high performance).
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public KeyDistributedScheduler(SimpleSchedulerInterface scheduler, int maxTasksPerCycle) {
    this(DEFAULT_LOCK_PARALISM, scheduler, maxTasksPerCycle, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.  
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be enabled 
   * if {@link #getTaskQueueSize(Object)} calls will be used.
   * 
   * This constructs with a default expected level of concurrency of 16. 
   * 
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedScheduler(SimpleSchedulerInterface scheduler, int maxTasksPerCycle, 
                                 boolean accurateQueueSize) {
    this(DEFAULT_LOCK_PARALISM, scheduler, maxTasksPerCycle, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * {@link #getTaskQueueSize(Object)} call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   */
  public KeyDistributedScheduler(int expectedParallism, SimpleSchedulerInterface scheduler) {
    this(expectedParallism, scheduler, Integer.MAX_VALUE, false);
  }
  
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to specify if you want accurate queue sizes to be tracked for 
   * given thread keys.  There is a performance hit associated with this, so this should only be 
   * enabled if {@link #getTaskQueueSize(Object)} calls will be used.
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedScheduler(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                 boolean accurateQueueSize) {
    this(expectedParallism, scheduler, Integer.MAX_VALUE, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.
   * 
   * This constructor does not attempt to have an accurate queue size for the 
   * {@link #getTaskQueueSize(Object)} call (thus preferring high performance).
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public KeyDistributedScheduler(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                 int maxTasksPerCycle) {
    this(expectedParallism, scheduler, maxTasksPerCycle, false);
  }
  
  /**
   * Constructor to use a provided scheduler implementation for running tasks.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.
   * 
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be enabled 
   * if {@link #getTaskQueueSize(Object)} calls will be used.
   * 
   * @param expectedParallism level of expected quantity of threads adding tasks in parallel
   * @param scheduler A multi-threaded scheduler to distribute tasks to.  Ideally has as many 
   *                  possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedScheduler(int expectedParallism, SimpleSchedulerInterface scheduler, 
                                 int maxTasksPerCycle, boolean accurateQueueSize) {
    this(scheduler, new StripedLock(expectedParallism), 
         maxTasksPerCycle, accurateQueueSize);
  }
  
  /**
   * Constructor to be used in unit tests.
   * 
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.
   * 
   * @param scheduler scheduler to be used for task worker execution 
   * @param sLock lock to be used for controlling access to workers
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  protected KeyDistributedScheduler(SimpleSchedulerInterface scheduler, StripedLock sLock, 
                                    int maxTasksPerCycle, boolean accurateQueueSize) {
    super(scheduler, sLock, maxTasksPerCycle, accurateQueueSize);
    
    this.scheduler = scheduler;
  }
  
  /**
   * Returns a scheduler implementation where all tasks submitted on this scheduler will run on 
   * the provided key.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @return scheduler which will only execute based on the provided key
   */
  public SubmitterSchedulerInterface getSubmitterSchedulerForKey(Object threadKey) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    
    return new KeyScheduler(threadKey);
  }

  /**
   * Schedule a one time task with a given delay that will not run concurrently based off the 
   * thread key.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to execute
   * @param delayInMs Time to wait to execute task
   */
  public void scheduleTask(Object threadKey, 
                           Runnable task, 
                           long delayInMs) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");
    
    if (delayInMs == 0) {
      addTask(threadKey, task, executor);
    } else {
      scheduler.schedule(new AddTask(threadKey, task), 
                         delayInMs);
    }
  }

  /**
   * Schedule a task with a given delay.  There is a slight increase in load when using 
   * {@link #submitScheduledTask(Object, Runnable, long)} over 
   * {@link #scheduleTask(Object, Runnable, long)}.  So this should only be used when the future 
   * is necessary.
   * 
   * The {@link ListenableFuture#get()} method will return {@code null} once the runnable has completed.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
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
   * Schedule a task with a given delay.  The future {@link ListenableFuture#get()} method will 
   * return null once the runnable has completed.
   * 
   * @param <T> type of result returned from the future
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task runnable to execute
   * @param result result to be returned from resulting {@link ListenableFuture#get()} when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public <T> ListenableFuture<T> submitScheduledTask(Object threadKey, Runnable task, 
                                                     T result, long delayInMs) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");

    ListenableRunnableFuture<T> rf = new ListenableFutureTask<T>(false, task, result);
    
    if (delayInMs == 0) {
      addTask(threadKey, rf, executor);
    } else {
      scheduler.schedule(new AddTask(threadKey, rf), delayInMs);
    }
    
    return rf;
  }

  /**
   * Schedule a {@link Callable} with a given delay.  This is needed when a result needs to be 
   * consumed from the callable.
   * 
   * @param <T> type of result returned from the future
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <T> ListenableFuture<T> submitScheduledTask(Object threadKey, 
                                                     Callable<T> task, 
                                                     long delayInMs) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(delayInMs, "delayInMs");

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
   * Schedule a fixed delay recurring task to run.  The recurring delay time will be from the 
   * point where execution has finished.  So the execution frequency is the 
   * {@code recurringDelay + runtime} for the provided task.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed.
   * @param initialDelay Delay in milliseconds until first run.
   * @param recurringDelay Delay in milliseconds for running task after last finish.
   */
  public void scheduleTaskWithFixedDelay(Object threadKey, Runnable task, 
                                         long initialDelay, long recurringDelay) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(recurringDelay, "recurringDelay");
    
    RecrringDelayTask rdt = new RecrringDelayTask(threadKey, task, recurringDelay);
    if (initialDelay == 0) {
      addTask(threadKey, rdt, executor);
    } else {
      scheduler.schedule(new AddTask(threadKey, rdt), 
                         initialDelay);
    }
  }
  
  /**
   * Schedule a fixed rate recurring task to run.  The recurring delay will be the same, 
   * regardless of how long task execution takes.  A given runnable will not run concurrently 
   * (unless it is submitted to the scheduler multiple times).  Instead of execution takes longer 
   * than the period, the next run will occur immediately (given thread availability in the pool).  
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task runnable to be executed
   * @param initialDelay delay in milliseconds until first run
   * @param period amount of time in milliseconds between the start of recurring executions
   */
  public void scheduleTaskAtFixedRate(Object threadKey, Runnable task, 
                                      long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    ArgumentVerifier.assertNotNegative(initialDelay, "initialDelay");
    ArgumentVerifier.assertNotNegative(period, "period");
    
    RecrringRateTask rrt = new RecrringRateTask(threadKey, task, period);
    if (initialDelay == 0) {
      addTask(threadKey, rrt, executor);
    } else {
      scheduler.schedule(new AddTask(threadKey, rrt), 
                         initialDelay);
    }
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
   * <p>Container for runnables which run with a fixed delay after the previous run.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecrringDelayTask implements Runnable, 
                                               RunnableContainerInterface {
    private final Object key;
    private final Runnable task;
    private final long recurringDelay;
    
    protected RecrringDelayTask(Object key, Runnable task, long recurringDelay) {
      this.key = key;
      this.task = task;
      this.recurringDelay = recurringDelay;
    }
    
    @Override
    public void run() {
      try {
        task.run();
      } finally {
        scheduler.schedule(new AddTask(key, this), recurringDelay);
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Container for runnables which run with a fixed rate, regardless of execution time.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class RecrringRateTask implements Runnable, 
                                              RunnableContainerInterface {
    private final Object key;
    private final Runnable task;
    private final long recurringPeriod;
    
    protected RecrringRateTask(Object key, Runnable task, long recurringPeriod) {
      this.key = key;
      this.task = task;
      this.recurringPeriod = recurringPeriod;
    }
    
    @Override
    public void run() {
      scheduler.schedule(new AddTask(key, this), recurringPeriod);
      task.run();
    }

    @Override
    public Runnable getContainedRunnable() {
      return task;
    }
  }
  
  /**
   * <p>Simple simple scheduler implementation that submits `xecutions and scheduling on a given 
   * key.</p>
   * 
   * @author jent - Mike Jensen
   * @since 2.5.0
   */
  protected class KeyScheduler extends KeySubmitter 
                               implements SubmitterSchedulerInterface {
    protected KeyScheduler(Object threadKey) {
      super(threadKey);
    }

    @Override
    public void schedule(Runnable task, long delayInMs) {
      KeyDistributedScheduler.this.scheduleTask(threadKey, task, 
                                                delayInMs);
    }

    @Override
    public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
      return submitScheduled(task, null, delayInMs);
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
      return KeyDistributedScheduler.this.submitScheduledTask(threadKey, task, result, delayInMs);
    }

    @Override
    public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
      return KeyDistributedScheduler.this.submitScheduledTask(threadKey, task, delayInMs);
    }

    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
      KeyDistributedScheduler.this.scheduleTaskWithFixedDelay(threadKey, task, 
                                                              initialDelay, 
                                                              recurringDelay);
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
      KeyDistributedScheduler.this.scheduleTaskAtFixedRate(threadKey, task, 
                                                           initialDelay, period);
    }
  }
}
