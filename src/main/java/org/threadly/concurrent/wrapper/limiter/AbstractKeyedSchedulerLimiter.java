package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>Abstract implementation for scheduled keyed limiters.  Unlike the other limiters we can not 
 * extend off of each one, and just add extra functionality.  Instead they must extend these 
 * abstract classes.  The reason for that being that these types of limiters use the other 
 * limiters, rather than extend functionality off each other.</p>
 * 
 * <p>This adds scheduling functionality.  It must exist in an abstract form because of the need 
 * to type the limiter type for specific functionality in the specific limiter type (casting is 
 * ugly, which would be the alternative).</p>
 * 
 * @param <T> Type of limiter stored internally
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 4.3.0 at org.threadly.concurrent.limiter)
 */
abstract class AbstractKeyedSchedulerLimiter<T extends SubmitterSchedulerLimiter> extends AbstractKeyedLimiter<T> {
  protected final SubmitterScheduler scheduler;
  
  protected AbstractKeyedSchedulerLimiter(SubmitterScheduler scheduler, int maxConcurrency, 
                                          String subPoolName, boolean addKeyToThreadName, 
                                          int expectedTaskAdditionParallism) {
    super(scheduler, maxConcurrency, subPoolName, addKeyToThreadName, expectedTaskAdditionParallism);
    
    this.scheduler = scheduler;
  }
  
  /**
   * 
   * Schedule a task with a given delay.  There is a slight increase in load when using 
   * {@link #submitScheduled(Object, Runnable, long)} over 
   * {@link #schedule(Object, Runnable, long)}.  So this should only be used when the future is 
   * necessary.
   * 
   * The {@link ListenableFuture#get()} method will return {@code null} once the runnable has 
   * completed.  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#submitScheduled(Runnable, long)}
   * 
   * @param taskKey Key to use for identifying execution limit
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public ListenableFuture<?> submitScheduled(Object taskKey, Runnable task, long delayInMs) {
    return submitScheduled(taskKey, task, null, delayInMs);
  }
  
  /**
   * Schedule a task with a given delay.  The {@link ListenableFuture#get()} method will return 
   * the provided result once the runnable has completed.  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#submitScheduled(Runnable, Object, long)}
   * 
   * @param <TT> type of result returned from the future
   * @param taskKey Key to use for identifying execution limit
   * @param task runnable to execute
   * @param result result to be returned from resulting future .get() when runnable completes
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed
   */
  public <TT> ListenableFuture<TT> submitScheduled(Object taskKey, Runnable task, TT result, long delayInMs) {
    return submitScheduled(taskKey, new RunnableCallableAdapter<>(task, result), delayInMs);
  }

  /**
   * Schedule a {@link Callable} with a given delay.  This is needed when a result needs to be 
   * consumed from the callable.  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#submitScheduled(Callable, long)}
   * 
   * @param <TT> type of result returned from the future
   * @param taskKey Key to use for identifying execution limit
   * @param task callable to be executed
   * @param delayInMs time in milliseconds to wait to execute task
   * @return a future to know when the task has completed and get the result of the callable
   */
  public <TT> ListenableFuture<TT> submitScheduled(Object taskKey, Callable<TT> task, long delayInMs) {
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableFutureTask<TT> ft = new ListenableFutureTask<>(false, task);
    
    schedule(taskKey, ft, delayInMs);
    
    return ft;
  }

  /**
   * Schedule a one time task with a given delay.  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#schedule(Runnable, long)}
   * 
   * @param taskKey Key to use for identifying execution limit
   * @param task runnable to execute
   * @param delayInMs time in milliseconds to wait to execute task
   */
  public void schedule(Object taskKey, Runnable task, long delayInMs) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    LimiterContainer lc = getLimiterContainer(taskKey);
    lc.limiter.doSchedule(lc.wrap(task), delayInMs);
  }
  
  /**
   * Schedule a fixed delay recurring task to run.  The recurring delay time will be from the 
   * point where execution has finished.  So the execution frequency is the 
   * {@code recurringDelay + runtime} for the provided task.  
   * 
   * Unlike {@link java.util.concurrent.ScheduledExecutorService} if the task throws an exception, 
   * subsequent executions are NOT suppressed or prevented.  So if the task throws an exception on 
   * every run, the task will continue to be executed at the provided recurring delay (possibly 
   * throwing an exception on each execution).  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#scheduleWithFixedDelay(Runnable, long, long)}
   * 
   * @param taskKey Key to use for identifying execution limit
   * @param task runnable to be executed
   * @param initialDelay delay in milliseconds until first run
   * @param recurringDelay delay in milliseconds for running task after last finish
   */
  public void scheduleWithFixedDelay(Object taskKey, Runnable task, long initialDelay,
                                     long recurringDelay) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    LimiterContainer lc = getLimiterContainer(taskKey);
    // we don't wrap the task here because it is recurring, this limiter can never be removed
    lc.limiter.scheduleWithFixedDelay(task, initialDelay, recurringDelay);
  }

  /**
   * Schedule a fixed rate recurring task to run.  The recurring delay will be the same, 
   * regardless of how long task execution takes.  A given runnable will not run concurrently 
   * (unless it is submitted to the scheduler multiple times).  Instead of execution takes longer 
   * than the period, the next run will occur immediately (given thread availability in the pool).  
   * 
   * Unlike {@link java.util.concurrent.ScheduledExecutorService} if the task throws an exception, 
   * subsequent executions are NOT suppressed or prevented.  So if the task throws an exception on 
   * every run, the task will continue to be executed at the provided recurring delay (possibly 
   * throwing an exception on each execution).  
   * 
   * The key is used to identify this threads execution limit.  Tasks with matching keys will be 
   * limited concurrent execution to the level returned by {@link #getMaxConcurrencyPerKey()}.
   * 
   * See also: {@link SubmitterScheduler#scheduleAtFixedRate(Runnable, long, long)}
   * 
   * @param taskKey Key to use for identifying execution limit
   * @param task runnable to be executed
   * @param initialDelay delay in milliseconds until first run
   * @param period amount of time in milliseconds between the start of recurring executions
   */
  public void scheduleAtFixedRate(Object taskKey, Runnable task, long initialDelay, long period) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    LimiterContainer lc = getLimiterContainer(taskKey);
    // we don't wrap the task here because it is recurring, this limiter can never be removed
    lc.limiter.scheduleAtFixedRate(task, initialDelay, period);
  }

  /**
   * Returns a scheduler implementation where all tasks submitted on this scheduler will run on 
   * the provided key.  Tasks executed on the returned scheduler will be limited by the key 
   * submitted on this instance equally with ones provided through the returned instance.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return scheduler which will only execute with reference to the provided key
   */
  public SubmitterScheduler getSubmitterSchedulerForKey(Object taskKey) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    return new KeyedSubmitterScheduler(taskKey);
  }

  /**
   * Submitter scheduler which delegates to this instance with a constructed permits and task key.
   * 
   * @since 4.9.0
   */
  protected class KeyedSubmitterScheduler extends AbstractSubmitterScheduler {
    protected final Object taskKey;
    
    protected KeyedSubmitterScheduler(Object taskKey) {
      this.taskKey = taskKey;
    }
    
    @Override
    public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
      AbstractKeyedSchedulerLimiter.this.scheduleWithFixedDelay(taskKey, task, 
                                                                initialDelay, recurringDelay);
    }

    @Override
    public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
      AbstractKeyedSchedulerLimiter.this.scheduleAtFixedRate(taskKey, task, initialDelay, period);
    }

    @Override
    protected void doSchedule(Runnable task, long delayInMs) {
      AbstractKeyedSchedulerLimiter.this.schedule(taskKey, task, delayInMs);
    }
  }
}
