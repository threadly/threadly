package org.threadly.concurrent.wrapper.limiter;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ImmediateResultListenableFuture;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.wrapper.PrioritySchedulerDefaultPriorityWrapper;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingSubmitterScheduler;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.StringUtils;

/**
 * Similar to {@link RateLimiterExecutor} except that the rate is applied on a per key basis.  
 * Tasks submitted to this executor must all be associated to a key.  The key is compared by using 
 * {@link Object#hashCode()} and {@link Object#equals(Object)}.  For any given key, a rate is 
 * applied, but keys which don't match with the above checks will not impact each other.
 * <p>
 * Because different keys don't interact, this is not capable of providing a global rate limit 
 * (unless the key quantity is known and restricted).
 * <p>
 * This differs from {@link KeyedExecutorLimiter} in that while that limits concurrency, this 
 * limits by rate (thus there may be periods where nothing is execution, or if executions are long 
 * things may run concurrently).  Please see {@link RateLimiterExecutor} for more details about how 
 * rate is limited.
 * 
 * @since 4.7.0
 */
public class KeyedRateLimiterExecutor {
  protected static final short DEFAULT_LOCK_PARALISM = 32;
  protected static final float CONCURRENT_HASH_MAP_LOAD_FACTOR = 0.75f;  // 0.75 is ConcurrentHashMap default
  protected static final short CONCURRENT_HASH_MAP_MIN_SIZE = 8;
  protected static final short CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE = 64;
  protected static final short CONCURRENT_HASH_MAP_MIN_CONCURRENCY_LEVEL = 4;
  protected static final short CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL = 32;
  
  protected final SubmitterScheduler scheduler;
  protected final RejectedExecutionHandler rejectedExecutionHandler;
  protected final SubmitterScheduler limiterCheckerScheduler;
  protected final double permitsPerSecond;
  protected final long maxScheduleDelayMillis;
  protected final String subPoolName;
  protected final boolean addKeyToThreadName;
  protected final StripedLock sLock;
  protected final ConcurrentHashMap<Object, RateLimiterExecutor> currentLimiters;
  
  /**
   * Constructs a new key rate limiting executor.  Using sensible default options.
   * <p>
   * This will schedule tasks out infinitely far in order to maintain rate.  If you want tasks to 
   * be rejected at a certain point consider using 
   * {@link #KeyedRateLimiterExecutor(SubmitterScheduler, double, long)}.
   * 
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond) {
    this(scheduler, permitsPerSecond, Long.MAX_VALUE, null, "", false, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  Allowing the specification of thread naming 
   * behavior.  Providing null or empty for the {@code subPoolName} and {@code false} for appending 
   * the key to the thread name will result in no thread name adjustments occurring.
   * <p>
   * This will schedule tasks out infinitely far in order to maintain rate.  If you want tasks to 
   * be rejected at a certain point consider using 
   * {@link #KeyedRateLimiterExecutor(SubmitterScheduler, double, long, String, boolean)}.
   * 
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param subPoolName Prefix to give threads while executing tasks submitted through this limiter
   * @param addKeyToThreadName {@code true} to append the task's key to the thread name
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  String subPoolName, boolean addKeyToThreadName) {
    this(scheduler, permitsPerSecond, Long.MAX_VALUE, null, 
         subPoolName, addKeyToThreadName, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  
   * <p>
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * 
   * @since 4.8.0
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  long maxScheduleDelayMillis) {
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, null, "", false, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  
   * <p>
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * 
   * @since 4.8.0
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  long maxScheduleDelayMillis, 
                                  RejectedExecutionHandler rejectedExecutionHandler) {
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, rejectedExecutionHandler, 
         "", false, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  Allowing the specification of thread naming 
   * behavior.  Providing null or empty for the {@code subPoolName} and {@code false} for appending 
   * the key to the thread name will result in no thread name adjustments occurring.    
   * <p>
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * 
   * @since 4.8.0
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   * @param subPoolName Prefix to give threads while executing tasks submitted through this limiter
   * @param addKeyToThreadName {@code true} to append the task's key to the thread name
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  long maxScheduleDelayMillis, 
                                  String subPoolName, boolean addKeyToThreadName) {
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, null, 
         subPoolName, addKeyToThreadName, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  Allowing the specification of thread naming 
   * behavior.  Providing null or empty for the {@code subPoolName} and {@code false} for appending 
   * the key to the thread name will result in no thread name adjustments occurring.    
   * <p>
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * 
   * @since 4.8.0
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed
   * @param subPoolName Prefix to give threads while executing tasks submitted through this limiter
   * @param addKeyToThreadName {@code true} to append the task's key to the thread name
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  long maxScheduleDelayMillis, 
                                  RejectedExecutionHandler rejectedExecutionHandler, 
                                  String subPoolName, boolean addKeyToThreadName) {
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, rejectedExecutionHandler, 
         subPoolName, addKeyToThreadName, DEFAULT_LOCK_PARALISM);
  }
  
  /**
   * Constructs a new key rate limiting executor.  This constructor allows you to set both the 
   * thread naming behavior as well as the level of parallelism expected for task submission.  
   * <p>
   * This constructor accepts a maximum schedule delay.  If a task requires being scheduled out 
   * beyond this delay, then a {@link java.util.concurrent.RejectedExecutionException} will be 
   * thrown instead of scheduling the task.
   * <p>
   * The parallelism value should be a factor of how many keys are submitted to the pool during any 
   * given period of time.  Depending on task execution duration, and quantity of threads executing 
   * tasks this value may be able to be smaller than expected.  Higher values result in less lock 
   * contention, but more memory usage.  Most systems will run fine with this anywhere from 4 to 64.
   * 
   * @since 4.8.0
   * @param scheduler Scheduler to defer executions to
   * @param permitsPerSecond how many permits should be allowed per second per key
   * @param maxScheduleDelayMillis Maximum amount of time delay tasks in order to maintain rate
   * @param rejectedExecutionHandler Handler to accept tasks which could not be executed
   * @param subPoolName Prefix to give threads while executing tasks submitted through this limiter
   * @param addKeyToThreadName {@code true} to append the task's key to the thread name
   * @param expectedParallism Expected level of task submission parallelism
   */
  public KeyedRateLimiterExecutor(SubmitterScheduler scheduler, double permitsPerSecond, 
                                  long maxScheduleDelayMillis, 
                                  RejectedExecutionHandler rejectedExecutionHandler, 
                                  String subPoolName, boolean addKeyToThreadName, 
                                  int expectedParallism) {
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    ArgumentVerifier.assertGreaterThanZero(permitsPerSecond, "permitsPerSecond");
    ArgumentVerifier.assertGreaterThanZero(maxScheduleDelayMillis, "maxScheduleDelayMillis");

    this.scheduler = scheduler;
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    if (scheduler instanceof PrioritySchedulerService) {
      limiterCheckerScheduler = 
          new PrioritySchedulerDefaultPriorityWrapper((PrioritySchedulerService)scheduler, 
                                                      TaskPriority.Low);
    } else {
      limiterCheckerScheduler = scheduler;
    }
    this.permitsPerSecond = permitsPerSecond;
    this.maxScheduleDelayMillis = maxScheduleDelayMillis;
    // make sure this is non-null so that it 'null' wont appear
    this.subPoolName = StringUtils.nullToEmpty(subPoolName);
    this.addKeyToThreadName = addKeyToThreadName;
    this.sLock = new StripedLock(expectedParallism);
    int mapInitialSize = Math.min(sLock.getExpectedConcurrencyLevel(), 
                                  CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE);
    if (mapInitialSize < CONCURRENT_HASH_MAP_MIN_SIZE) {
      mapInitialSize = CONCURRENT_HASH_MAP_MIN_SIZE;
    }
    int mapConcurrencyLevel = Math.max(CONCURRENT_HASH_MAP_MIN_CONCURRENCY_LEVEL, 
                                       Math.min(sLock.getExpectedConcurrencyLevel() / 2, 
                                                CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL));
    if (mapConcurrencyLevel < 1) {
      mapConcurrencyLevel = 1;
    }
    this.currentLimiters = new ConcurrentHashMap<>(mapInitialSize,  
                                                   CONCURRENT_HASH_MAP_LOAD_FACTOR, 
                                                   mapConcurrencyLevel);
  }
  
  /**
   * Check how many keys are currently being restricted or monitored.  This number is particularly 
   * relevant for when checking the queued tasks of the parent scheduler.  As part of the inner 
   * workings of this limiter, a task will exist for each key.  Because of that there will be 
   * queued tasks which are not actual application submitted work units.
   * 
   * @return The number of task keys being monitored
   */
  public int getTrackedKeyCount() {
    return currentLimiters.size();
  }
  
  /**
   * This call will check how far out we have already scheduled tasks to be run.  Because it is 
   * the applications responsibility to not provide tasks too fast for the limiter to run them, 
   * this can give an idea of how backed up tasks provided through this limiter actually are.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return minimum delay in milliseconds for the next task to be provided
   */
  public int getMinimumDelay(Object taskKey) {
    RateLimiterExecutor rle = currentLimiters.get(taskKey);
    if (rle == null) {
      return 0;
    } else {
      return rle.getMinimumDelay();
    }
  }
  
  /**
   * In order to help assist with avoiding to schedule too much on the scheduler at any given 
   * time, this call returns a future that will block until the delay for the next task falls 
   * below the maximum delay provided into this call.  If you want to ensure that the next task 
   * will execute immediately, you should provide a zero to this function.  If more tasks are 
   * added to the limiter after this call, it will NOT impact when this future will unblock.  So 
   * this future is assuming that nothing else is added to the limiter after requested.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param maximumDelay maximum delay in milliseconds until returned Future should unblock
   * @return Future that will unblock {@code get()} calls once delay has been reduced below the provided maximum
   */
  public ListenableFuture<?> getFutureTillDelay(Object taskKey, long maximumDelay) {
    int currentMinimumDelay = getMinimumDelay(taskKey);
    if (currentMinimumDelay == 0) {
      return ImmediateResultListenableFuture.NULL_RESULT;
    } else {
      ListenableFutureTask<?> lft = new ListenableFutureTask<>(false, DoNothingRunnable.instance());
      
      long futureDelay;
      if (maximumDelay > 0 && currentMinimumDelay > maximumDelay) {
        futureDelay = maximumDelay;
      } else {
        futureDelay = currentMinimumDelay;
      }
      
      scheduler.schedule(lft, futureDelay);
      
      return lft;
    }
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#execute(Runnable)} and 
   * {@link RateLimiterExecutor#execute(Runnable)}.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   */
  public void execute(Object taskKey, Runnable task) {
    execute(1, taskKey, task);
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#execute(Runnable)} and 
   * {@link RateLimiterExecutor#execute(double, Runnable)}.
   * 
   * @param permits resource permits for this task
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   * @return Time in milliseconds task was delayed to maintain rate, or {@code -1} if rejected but handler did not throw
   */
  public long execute(double permits, Object taskKey, Runnable task) {
    ArgumentVerifier.assertNotNegative(permits, "permits");
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    return doExecute(permits, taskKey, task);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Runnable)} and 
   * {@link RateLimiterExecutor#submit(Runnable)}.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   * @return Future to represent when the execution has occurred
   */
  public ListenableFuture<?> submit(Object taskKey, Runnable task) {
    return submit(1, taskKey, task);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Runnable)} and 
   * {@link RateLimiterExecutor#submit(double, Runnable)}.
   * 
   * @param permits resource permits for this task
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   * @return Future to represent when the execution has occurred
   */
  public ListenableFuture<?> submit(double permits, Object taskKey, Runnable task) {
    return submit(permits, taskKey, task, null);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Runnable, Object)} and 
   * {@link RateLimiterExecutor#submit(Runnable, Object)}.
   * 
   * @param <T> type of result returned from the future
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Runnable to be executed
   * @param result Result to be returned from future when task completes
   * @return Future to represent when the execution has occurred and provide the given result
   */
  public <T> ListenableFuture<T> submit(Object taskKey, Runnable task, T result) {
    return submit(1, taskKey, task, result);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Runnable, Object)} and 
   * {@link RateLimiterExecutor#submit(double, Runnable, Object)}.
   * 
   * @param <T> type of result returned from the future
   * @param permits resource permits for this task
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Runnable to be executed
   * @param result Result to be returned from future when task completes
   * @return Future to represent when the execution has occurred and provide the given result
   */
  public <T> ListenableFuture<T> submit(double permits, Object taskKey, Runnable task, T result) {
    return submit(permits, taskKey, new RunnableCallableAdapter<>(task, result));
  }
  
  /**
   * Submit a callable to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Callable)} and 
   * {@link RateLimiterExecutor#submit(Callable)}.
   * 
   * @param <T> type of result returned from the future
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Callable to be executed
   * @return Future to represent when the execution has occurred and provide the result from the callable
   */
  public <T> ListenableFuture<T> submit(Object taskKey, Callable<T> task) {
    return submit(1, taskKey, task);
  }
  
  /**
   * Submit a callable to be run with a given thread key.
   * <p>
   * See also: {@link SubmitterExecutor#submit(Callable)} and 
   * {@link RateLimiterExecutor#submit(double, Callable)}.
   * 
   * @param <T> type of result returned from the future
   * @param permits resource permits for this task
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Callable to be executed
   * @return Future to represent when the execution has occurred and provide the result from the callable
   */
  public <T> ListenableFuture<T> submit(double permits, Object taskKey, Callable<T> task) {
    ArgumentVerifier.assertNotNegative(permits, "permits");
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableRunnableFuture<T> rf = new ListenableFutureTask<>(false, task);
    
    doExecute(permits, taskKey, rf);
    
    return rf;
  }
  
  /**
   * Internal call to actually execute prepared runnable.
   * 
   * @param permits resource permits for this task
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Runnable to execute when ready
   * @return Time in milliseconds task was delayed to maintain rate, or {@code -1} if rejected but handler did not throw
   */
  protected long doExecute(double permits, Object taskKey, Runnable task) {
    RateLimiterExecutor rle;
    Object lock = sLock.getLock(taskKey);
    synchronized (lock) {
      rle = currentLimiters.get(taskKey);
      if (rle == null) {
        String keyedPoolName = subPoolName + (addKeyToThreadName ? taskKey.toString() : "");
        SubmitterScheduler threadNamedScheduler;
        if (StringUtils.isNullOrEmpty(keyedPoolName)) {
          threadNamedScheduler = scheduler;
        } else {
          threadNamedScheduler = new ThreadRenamingSubmitterScheduler(scheduler, keyedPoolName, false);
        }
        rle = new RateLimiterExecutor(threadNamedScheduler, permitsPerSecond, 
                                      maxScheduleDelayMillis, rejectedExecutionHandler);

        currentLimiters.put(taskKey, rle);
        // schedule task to check for removal later, should only be one task per limiter
        limiterCheckerScheduler.schedule(new LimiterChecker(taskKey, rle), 1000);
      }
      
      // must execute while in lock to prevent early removal
      return rle.execute(permits, task);
    }
  }

  /**
   * Returns an executor implementation where all tasks submitted on this executor will run on the 
   * provided key.  Tasks executed on the returned scheduler will be limited by the key 
   * submitted on this instance equally with ones provided through the returned instance.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return Executor which will only execute with reference to the provided key
   */
  public SubmitterExecutor getSubmitterExecutorForKey(Object taskKey) {
    return getSubmitterExecutorForKey(1, taskKey);
  }

  /**
   * Returns an executor implementation where all tasks submitted on this executor will run on the 
   * provided key.  Tasks executed on the returned scheduler will be limited by the key 
   * submitted on this instance equally with ones provided through the returned instance.
   * 
   * @param permits resource permits for all tasks submitted on the returned executor
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return Executor which will only execute with reference to the provided key
   */
  public SubmitterExecutor getSubmitterExecutorForKey(double permits, Object taskKey) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    return new KeyedSubmitterExecutor(permits, taskKey);
  }
  
  /**
   * Submitter executor which delegates to this instance with a constructed permits and task key.
   * 
   * @since 4.9.0
   */
  protected class KeyedSubmitterExecutor extends AbstractSubmitterExecutor {
    protected final double permits;
    protected final Object taskKey;
    
    protected KeyedSubmitterExecutor(double permits, Object taskKey) {
      this.permits = permits;
      this.taskKey = taskKey;
    }
    
    @Override
    protected void doExecute(Runnable task) {
      KeyedRateLimiterExecutor.this.doExecute(permits, taskKey, task);
    }
  }
  
  /**
   * Task which checks to see if a limiter has become idle.  If so it removes the limiter in a 
   * thread safe way.  If the limiter is still active then it will schedule itself to check again 
   * later.
   * 
   * @since 4.7.0
   */
  protected class LimiterChecker implements Runnable {
    public final Object taskKey;
    public final RateLimiterExecutor limiter;
    
    public LimiterChecker(Object taskKey, RateLimiterExecutor limiter) {
      this.taskKey = taskKey;
      this.limiter = limiter;
    }

    @Override
    public void run() {
      int minimumDelay;
      synchronized (sLock.getLock(taskKey)) {
        minimumDelay = limiter.getMinimumDelay();
        if (minimumDelay == 0) {
          currentLimiters.remove(taskKey);
          return;
        }
      }
      
      // did not return above, so reschedule our check
      limiterCheckerScheduler.schedule(this, minimumDelay + 100); // add a little to encourage object reuse
    }
  }
}
