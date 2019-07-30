package org.threadly.concurrent.wrapper.limiter;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PrioritySchedulerService;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ImmediateResultListenableFuture;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.priority.DefaultPriorityWrapper;
import org.threadly.concurrent.wrapper.traceability.ThreadRenamingSubmitterScheduler;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
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
  protected static final short LIMITER_IDLE_TIMEOUT = 2_000;
  protected static final short CONCURRENT_HASH_MAP_INITIAL_SIZE = 16;
  
  protected final SubmitterScheduler scheduler;
  protected final RejectedExecutionHandler rejectedExecutionHandler;
  protected final SubmitterScheduler limiterCheckerScheduler;
  protected final double permitsPerSecond;
  protected final long maxScheduleDelayMillis;
  protected final String subPoolName;
  protected final boolean addKeyToThreadName;
  protected final ConcurrentHashMap<Object, RateLimiterExecutor> currentLimiters;
  protected final LimiterChecker limiterChecker;
  
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
    this(scheduler, permitsPerSecond, Long.MAX_VALUE, null, "", false);
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
         subPoolName, addKeyToThreadName);
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
    this(scheduler, permitsPerSecond, maxScheduleDelayMillis, null, "", false);
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
         "", false);
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
         subPoolName, addKeyToThreadName);
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
    ArgumentVerifier.assertNotNull(scheduler, "scheduler");
    ArgumentVerifier.assertGreaterThanZero(permitsPerSecond, "permitsPerSecond");
    ArgumentVerifier.assertGreaterThanZero(maxScheduleDelayMillis, "maxScheduleDelayMillis");

    this.scheduler = scheduler;
    this.rejectedExecutionHandler = rejectedExecutionHandler;
    if (scheduler instanceof PrioritySchedulerService) {
      limiterCheckerScheduler = 
          DefaultPriorityWrapper.ensurePriority((PrioritySchedulerService)scheduler, 
                                                TaskPriority.Low);
    } else {
      limiterCheckerScheduler = scheduler;
    }
    this.permitsPerSecond = permitsPerSecond;
    this.maxScheduleDelayMillis = maxScheduleDelayMillis;
    // make sure this is non-null so that it 'null' wont appear
    this.subPoolName = StringUtils.nullToEmpty(subPoolName);
    this.addKeyToThreadName = addKeyToThreadName;
    this.currentLimiters = new ConcurrentHashMap<>(CONCURRENT_HASH_MAP_INITIAL_SIZE);
    this.limiterChecker = new LimiterChecker(scheduler, LIMITER_IDLE_TIMEOUT / 2);
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
      long futureDelay;
      if (maximumDelay > 0 && currentMinimumDelay > maximumDelay) {
        futureDelay = maximumDelay;
      } else {
        futureDelay = currentMinimumDelay;
      }
      
      return scheduler.submitScheduled(DoNothingRunnable.instance(), futureDelay);
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
    return limiterForKey(taskKey, (l) -> l.execute(permits, task));
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
    return submit(1, taskKey, task, null);
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
    // we go directly to the limiter here to get DoNothingRunnable optimizations (can't wrap task)
    return limiterForKey(taskKey, (l) -> l.submit(permits, task, result));
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
    return limiterForKey(taskKey, (l) -> l.submit(permits, task));
  }
  
  /**
   * This invokes a function with a provided {@link RateLimiterExecutor}.  This invocation is 
   * atomic using {@link ConcurrentHashMap#compute(Object, java.util.function.BiFunction)} so that 
   * it will not interfere with others.
   * 
   * @param <T> Type of result from provided function
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return A {@link RateLimiterExecutor} that is shared by the key
   */
  @SuppressWarnings("unchecked")
  protected <T> T limiterForKey(Object taskKey, Function<RateLimiterExecutor, ? extends T> c) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    Object[] capture = new Object[1];
    currentLimiters.compute(taskKey, (k, v) -> {
      if (v == null) {
        String keyedPoolName = subPoolName + (addKeyToThreadName ? taskKey.toString() : "");
        SubmitterScheduler threadNamedScheduler;
        if (StringUtils.isNullOrEmpty(keyedPoolName)) {
          threadNamedScheduler = scheduler;
        } else {
          threadNamedScheduler = new ThreadRenamingSubmitterScheduler(scheduler, keyedPoolName, false);
        }
        v = new RateLimiterExecutor(threadNamedScheduler, permitsPerSecond, 
                                    maxScheduleDelayMillis, rejectedExecutionHandler);
        limiterChecker.signalToRun();
      }
      // TODO - I would like to improve this
      //          This is awkward having to construct an Object[] just to get the result returned
      //          Ideally we would avoid both that, and the lambda's that need to be passed in, but 
      //          how to do that is not super obvious.  By applying the function inside the `compute` 
      //          we are ensuring the operation is atomic compared to any removes that might want 
      //          to happen.
      //          Other ideas I have:
      //            * Increase memory overhead by having the value be an Object[] of size 2
      //                Index 0 would be the RateLimiterExecutor, 1 would be able to be used for the capture
      //            * Extend RateLimiterExecutor and store a timestamp that is updated here in compute.
      //                This is probably the simplest, but does not completely solve the problem
      //                This would just ensure the timestamp is updated when looking to remove, but 
      //                If the action took X milliseconds after the update, then it may still be removed.
      //                Highly unlikely, but I prefer impossibility if we can
      capture[0] = c.apply(v);
      return v;
    });
    return (T)capture[0];
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
    ArgumentVerifier.assertNotNegative(permits, "permits");
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
      limiterForKey(taskKey, (l) -> l.execute(permits, task));
    }
  }
  
  /**
   * Task which checks over all limiters to see if any should be expired / removed.
   * 
   * @since 5.25
   */
  protected class LimiterChecker extends ReschedulingOperation {
    protected LimiterChecker(SubmitterScheduler scheduler, long scheduleDelay) {
      super(scheduler, scheduleDelay);
    }

    @Override
    public void run() {
      Iterator<Map.Entry<Object, RateLimiterExecutor>> it = currentLimiters.entrySet().iterator();
      long now = Clock.lastKnownForwardProgressingMillis();
      while (it.hasNext()) {
        Map.Entry<Object, RateLimiterExecutor> e = it.next();
        if (now - e.getValue().getLastScheduleTime() > LIMITER_IDLE_TIMEOUT) {
          // if optimistic check above failed, we must remove in `compute` to ensure
          // no tasks are being submitted while we are removing the limiter
          currentLimiters.computeIfPresent(e.getKey(), (k, v) -> {
            if (now - v.getLastScheduleTime() > LIMITER_IDLE_TIMEOUT) {
              return null;
            } else {
              return v;
            }
          });
        }
      }
      if (! currentLimiters.isEmpty()) {
        signalToRun();
      }
    }
  }
}
