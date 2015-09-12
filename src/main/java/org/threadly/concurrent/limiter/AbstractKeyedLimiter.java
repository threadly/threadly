package org.threadly.concurrent.limiter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.StringUtils;

/**
 * <p>Abstract implementation for keyed limiters.  Unlike the other limiters we can not extend off 
 * of each one, and just add extra functionality.  Instead they must extend these abstract classes.  
 * The reason for that being that these types of limiters use the other limiters, rather than 
 * extend functionality off each other.</p>
 * 
 * @param <T> Type of limiter stored internally
 * @author jent - Mike Jensen
 * @since 4.3.0
 */
abstract class AbstractKeyedLimiter<T extends ExecutorLimiter> {
  protected static final int DEFAULT_LOCK_PARALISM = 32;
  protected static final float CONCURRENT_HASH_MAP_LOAD_FACTOR = 0.75f;  // 0.75 is ConcurrentHashMap default
  protected static final int CONCURRENT_HASH_MAP_MIN_SIZE = 8;
  protected static final int CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE = 64;
  protected static final int CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL = 32;
  
  protected final Executor executor;
  protected final int maxConcurrency;
  protected final String subPoolName;
  protected final boolean addKeyToThreadName;
  protected final StripedLock sLock;
  protected final ConcurrentHashMap<Object, LimiterContainer> currentLimiters;
  
  protected AbstractKeyedLimiter(Executor executor, int maxConcurrency, 
                                 String subPoolName, boolean addKeyToThreadName, 
                                 int expectedTaskAdditionParallism) {
    ArgumentVerifier.assertGreaterThanZero(maxConcurrency, "maxConcurrency");
    ArgumentVerifier.assertNotNull(executor, "executor");

    this.executor = executor;
    this.maxConcurrency = maxConcurrency;
    // make sure this is non-null so that it 'null' wont appear
    this.subPoolName = StringUtils.nullToEmpty(subPoolName);
    this.addKeyToThreadName = addKeyToThreadName;
    this.sLock = new StripedLock(expectedTaskAdditionParallism);
    int mapInitialSize = Math.min(sLock.getExpectedConcurrencyLevel(), 
                                  CONCURRENT_HASH_MAP_MAX_INITIAL_SIZE);
    if (mapInitialSize < CONCURRENT_HASH_MAP_MIN_SIZE) {
      mapInitialSize = CONCURRENT_HASH_MAP_MIN_SIZE;
    }
    int mapConcurrencyLevel = Math.min(sLock.getExpectedConcurrencyLevel() / 2, 
                                       CONCURRENT_HASH_MAP_MAX_CONCURRENCY_LEVEL);
    if (mapConcurrencyLevel < 1) {
      mapConcurrencyLevel = 1;
    }
    this.currentLimiters = new ConcurrentHashMap<Object, LimiterContainer>(mapInitialSize,  
                                                                           CONCURRENT_HASH_MAP_LOAD_FACTOR, 
                                                                           mapConcurrencyLevel);
  }
  
  /**
   * Check how many threads may run in parallel for a single unique key.
   * 
   * @return maximum concurrent tasks to be run
   */
  public int getMaxConcurrencyPerKey() {
    return maxConcurrency;
  }
  
  /**
   * Check how many tasks are currently being limited, and not submitted yet for a given key.  
   * This can be useful for knowing how backed up a specific key is.
   * 
   * @param taskKey Key which would be limited
   * @return Quantity of tasks being held back inside the limiter, and thus still queued
   */
  public int getUnsubmittedTaskCount(Object taskKey) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    LimiterContainer lc = currentLimiters.get(taskKey);
    return lc == null ? 0 : lc.limiter.getUnsubmittedTaskCount();
  }
  
  /**
   * Get a map of all the keys and how many tasks are held back (queued) in each limiter per key.  
   * This map is generated without locking.  Due to that, this may be inaccurate as task queue 
   * sizes changed while iterating all key's limiters.
   * 
   * Because this requires an iteration of all limiters, if only a single limiters unsubmitted 
   * count is needed, use {@link #getUnsubmittedTaskCount(Object)} as a cheaper alternative.
   * 
   * @return Map of task key's to their respective task queue size
   */
  public Map<Object, Integer> getUnsubmittedTaskCountMap() {
    Map<Object, Integer> result = new HashMap<Object, Integer>();
    for (Map.Entry<Object, LimiterContainer> e : currentLimiters.entrySet()) {
      int taskCount = e.getValue().limiter.getUnsubmittedTaskCount();
      if (taskCount > 0) {
        result.put(e.getKey(), taskCount);
      }
    }
    return result;
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * 
   * See also: {@link SubmitterExecutor#execute(Runnable)}
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   */
  public void execute(Object taskKey, Runnable task) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    getLimiterContainer(taskKey).execute(task);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * See also: {@link SubmitterExecutor#submit(Runnable)}
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   * @return Future to represent when the execution has occurred
   */
  public ListenableFuture<?> submit(Object taskKey, Runnable task) {
    return submit(taskKey, task, null);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * See also: {@link SubmitterExecutor#submit(Runnable, Object)}
   * 
   * @param <TT> type of result returned from the future
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Runnable to be executed
   * @param result Result to be returned from future when task completes
   * @return Future to represent when the execution has occurred and provide the given result
   */
  public <TT> ListenableFuture<TT> submit(Object taskKey, Runnable task, TT result) {
    return submit(taskKey, new RunnableCallableAdapter<TT>(task, result));
  }
  
  /**
   * Submit a callable to be run with a given thread key.
   * 
   * See also: {@link SubmitterExecutor#submit(Callable)}
   * 
   * @param <TT> type of result returned from the future
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @param task Callable to be executed
   * @return Future to represent when the execution has occurred and provide the result from the callable
   */
  public <TT> ListenableFuture<TT> submit(Object taskKey, Callable<TT> task) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableRunnableFuture<TT> rf = new ListenableFutureTask<TT>(false, task);
    
    getLimiterContainer(taskKey).execute(rf);
    
    return rf;
  }
  
  /**
   * Get the current limiter in a thread safe way.  If the limiter does not exist it will be 
   * created in a thread safe way.  In addition the limiters handling task count will be 
   * incremented in expectation for execution.  If not accessing for execution 
   * {@link #currentLimiters} should just be accessed directly.
   * 
   * @param taskKey Key used to identify execution limiter
   * @return Container with limiter and associated state data
   */
  protected LimiterContainer getLimiterContainer(Object taskKey) {
    LimiterContainer lc;
    Object lock = sLock.getLock(taskKey);
    synchronized (lock) {
      lc = currentLimiters.get(taskKey);
      if (lc == null) {
        lc = new LimiterContainer(taskKey, makeLimiter(subPoolName + 
                                                         (addKeyToThreadName ? taskKey.toString() : "")));
        currentLimiters.put(taskKey, lc);
      }
      // must increment while in lock to prevent early removal
      lc.handlingTasks.incrementAndGet();
    }
    
    return lc;
  }
  
  /**
   * Constructs a new limiter that is specific for the given type.
   * 
   * @param limiterThreadName Name for threads inside subpool
   * @return A newly constructed limiter
   */
  protected abstract T makeLimiter(String limiterThreadName);

  /**
   * Returns an executor implementation where all tasks submitted on this executor will run on the 
   * provided key.  Tasks executed on the returned scheduler will be limited by the key 
   * submitted on this instance equally with ones provided through the returned instance.
   * 
   * @param taskKey object key where {@code equals()} will be used to determine execution thread
   * @return Executor which will only execute with reference to the provided key
   */
  public SubmitterExecutor getSubmitterExecutorForKey(final Object taskKey) {
    ArgumentVerifier.assertNotNull(taskKey, "taskKey");
    
    return new AbstractSubmitterExecutor() {
      @Override
      protected void doExecute(Runnable task) {
        getLimiterContainer(taskKey).execute(task);
      }
    };
  }
  
  /**
   * <p>Small class to hold the limiter and state associated with the limiter.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.3.0
   */
  protected class LimiterContainer {
    public final Object taskKey;
    public final T limiter;
    public final AtomicInteger handlingTasks;
    
    public LimiterContainer(Object taskKey, T limiter) {
      this.taskKey = taskKey;
      this.limiter = limiter;
      this.handlingTasks = new AtomicInteger(0);
    }

    public Runnable wrap(Runnable task) {
      return new LimiterCleaner(task);
    }
    
    public void execute(Runnable task) {
      limiter.doExecute(wrap(task));
    }
    
    /**
     * <p>Small class to handle tracking as tasks finish.  Once the last task of a limiter finishes 
     * the limiter is removed for GC.  This wraps the runnable to handle that cleanup if needed.</p>
     * 
     * @author jent - Mike Jensen
     * @since 4.3.0
     */
    private class LimiterCleaner implements Runnable, RunnableContainer {
      private final Runnable wrappedTask;
      
      protected LimiterCleaner(Runnable wrappedTask) {
        this.wrappedTask = wrappedTask;
      }
      
      @Override
      public void run() {
        try {
          wrappedTask.run();
        } finally {
          if (handlingTasks.decrementAndGet() == 0) {
            synchronized (sLock.getLock(taskKey)) {
              // must verify removal in lock so that map gets are atomic with removals
              if (handlingTasks.get() == 0) {
                currentLimiters.remove(taskKey);
              }
            }
          }
        }
      }

      @Override
      public Runnable getContainedRunnable() {
        return wrappedTask;
      }
    }
  }
}
