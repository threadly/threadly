package org.threadly.concurrent.wrapper;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;

import org.threadly.concurrent.RunnableCallableAdapter;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * TaskDistributor is designed such that tasks executed on it for a given key will run in a single 
 * threaded manner.  It needs a multi-threaded pool supplied to it, to then execute those tasks 
 * on.  While the thread which runs those tasks may be different between multiple executions, no 
 * two tasks for the same key will ever be run in parallel.
 * <p>
 * Because of that, it is recommended that the executor provided has as many possible threads as 
 * possible keys that could be provided to be run in parallel.  If this class is starved for 
 * threads some keys may continue to process new tasks, while other keys could be starved.
 * <p>
 * Assuming that the shared memory (any objects, primitives, etc) are only accessed through the 
 * same instance of {@link KeyDistributedExecutor}, and assuming that those variables are only 
 * accessed via the same key.  Then the programmer does not need to worry about synchronization, or 
 * volatile.  The {@link KeyDistributedExecutor} will ensure the happens-before relationship.
 * 
 * @since 4.6.0 (since 1.0.0 as org.threadly.concurrent.TaskExecutorDistributor)
 */
public class KeyDistributedExecutor {
  protected static final short CONCURRENT_HASH_MAP_INITIAL_SIZE = 16;
  protected static final short ARRAY_DEQUE_INITIAL_SIZE = 8;  // minimum is 8, should be 2^X
  
  protected final Executor executor;
  protected final int maxTasksPerCycle;
  protected final BiFunction<Object, Runnable, TaskQueueWorker> wFactory;
  protected final ConcurrentHashMap<Object, TaskQueueWorker> taskWorkers;
  
  /**
   * Constructor to use a provided executor implementation for running tasks.  
   * <p>
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the {@link #getTaskQueueSize(Object)} call (thus 
   * preferring high performance).
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  Ideally has as many 
   *                 possible threads as keys that will be used in parallel. 
   */
  public KeyDistributedExecutor(Executor executor) {
    this(executor, Integer.MAX_VALUE, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.  
   * <p>
   * This constructor allows you to specify if you want accurate queue sizes to be tracked for 
   * given thread keys.  There is a performance hit associated with this, so this should only be 
   * enabled if {@link #getTaskQueueSize(Object)} calls will be used.  
   * <p>
   * This constructs with a default expected level of concurrency of 16.
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  Ideally has as many 
   *                 possible threads as keys that will be used in parallel.
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedExecutor(Executor executor, boolean accurateQueueSize) {
    this(executor, Integer.MAX_VALUE, accurateQueueSize);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * <p>
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.  
   * <p>
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be enabled 
   * if {@link #getTaskQueueSize(Object)} calls will be used.  
   * <p>
   * This constructs with a default expected level of concurrency of 16.  This also does not 
   * attempt to have an accurate queue size for the "getTaskQueueSize" call (thus preferring 
   * high performance).
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  Ideally has as many 
   *                 possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   */
  public KeyDistributedExecutor(Executor executor, int maxTasksPerCycle) {
    this(executor, maxTasksPerCycle, false);
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * <p>
   * This constructor allows you to provide a maximum number of tasks for a key before it yields 
   * to another key.  This can make it more fair, and make it so no single key can starve other 
   * keys from running.  The lower this is set however, the less efficient it becomes in part 
   * because it has to give up the thread and get it again, but also because it must copy the 
   * subset of the task queue which it can run.  
   * <p>
   * This also allows you to specify if you want accurate queue sizes to be tracked for given 
   * thread keys.  There is a performance hit associated with this, so this should only be enabled 
   * if {@link #getTaskQueueSize(Object)} calls will be used.
   * <p>
   * This constructs with a default expected level of concurrency of 16. 
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  Ideally has as many 
   *                 possible threads as keys that will be used in parallel.
   * @param maxTasksPerCycle maximum tasks run per key before yielding for other keys
   * @param accurateQueueSize {@code true} to make {@link #getTaskQueueSize(Object)} more accurate
   */
  public KeyDistributedExecutor(Executor executor, int maxTasksPerCycle, 
                                boolean accurateQueueSize) {
    ArgumentVerifier.assertNotNull(executor, "executor");
    ArgumentVerifier.assertGreaterThanZero(maxTasksPerCycle, "maxTasksPerCycle");
    
    this.executor = executor;
    this.maxTasksPerCycle = maxTasksPerCycle;
    if (accurateQueueSize) {
      wFactory = StatisticWorker::new;
    } else {
      wFactory = TaskQueueWorker::new;
    }
    this.taskWorkers = new ConcurrentHashMap<>(CONCURRENT_HASH_MAP_INITIAL_SIZE);
  }
  
  /**
   * Getter for the executor being used behind the scenes.
   * 
   * @return executor tasks are being distributed to
   */
  public Executor getExecutor() {
    return executor;
  }
  
  /**
   * Returns a {@link SubmitterExecutor} implementation where all tasks submitted on this executor 
   * will run on the provided key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @return executor which will only execute based on the provided key
   */
  public SubmitterExecutor getExecutorForKey(Object threadKey) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    
    return new KeySubmitter(threadKey);
  }
  
  /**
   * Call to check how many tasks have been queued up for a given key.  Depending on what 
   * constructor was used, and if a {@code true} was passed in for {@code accurateQueueSize}, the 
   * accuracy of this call varies dramatically.
   * <p>
   * If {@code true} was not supplied in the constructor for {@code accurateQueueSize}, this will 
   * only report how many tasks have not been accepted by the worker yet.  The accepting of those 
   * tasks occur in batches, so this number will vary dramatically (and probably be unusable).
   * <p>
   * So it is highly recommended that if your interested in this functionality you supply a 
   * {@code true} into the constructor.
   * <p>
   * Supplying a {@code true} for {@code accurateQueueSize} in the constructor does involve some 
   * performance cost, but that overhead should be minimal (just no reason to accept any loss if 
   * not interested in this feature).
   * 
   * @since 1.2.0
   * @param threadKey key for task queue to examine
   * @return the number of tasks queued for the key
   */
  public int getTaskQueueSize(Object threadKey) {
    TaskQueueWorker worker = taskWorkers.get(threadKey);
    if (worker == null) {
      return 0;
    } else {
      return worker.getQueueSize();
    }
  }
  
  /**
   * Get a map of all the keys and how many tasks are queued per key.  This map is generated 
   * without locking.  Due to that, this may be inaccurate as task queue sizes changed while 
   * iterating all key's active workers.
   * <p>
   * Because this requires an iteration of all task workers, if only a single key's queue size is 
   * needed, use {@link #getTaskQueueSize(Object)} as a cheaper alternative.
   * <p>
   * If {@code true} was not supplied in the constructor for {@code accurateQueueSize}, this will 
   * only report how many tasks have not been accepted by the worker yet.  The accepting of those 
   * tasks occur in batches, so this number will vary dramatically (and probably be unusable).
   * <p>
   * So it is highly recommended that if your interested in this functionality you supply a 
   * {@code true} into the constructor.
   * <p>
   * Supplying a {@code true} for {@code accurateQueueSize} in the constructor does involve some 
   * performance cost, but that overhead should be minimal (just no reason to accept any loss if 
   * not interested in this feature).
   * 
   * @return Map of task key's to their respective queue size
   */
  public Map<Object, Integer> getTaskQueueSizeMap() {
    Map<Object, Integer> result = new HashMap<>();
    for (Map.Entry<Object, TaskQueueWorker> e : taskWorkers.entrySet()) {
      result.put(e.getKey(), e.getValue().getQueueSize());
    }
    return result;
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   */
  public void execute(Object threadKey, Runnable task) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    addTask(threadKey, task, executor);
  }
  
  /**
   * This is a protected implementation to add the task to a worker.  No safety checks are done at 
   * this point, so only provide non-null inputs.
   * <p>
   * You can supply the executor in case extending classes want to use different executors than 
   * the class was constructed with.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be added to worker
   * @param Executor to run worker on (if it needs to be started)
   */
  protected void addTask(Object threadKey, Runnable task, Executor executor) {
    boolean[] startCapture = new boolean[1];
    TaskQueueWorker worker = taskWorkers.compute(threadKey, (k, v) -> {
      if (v == null) {
        startCapture[0] = true;
        v = wFactory.apply(threadKey, task);
      } else {
        startCapture[0] = false;
        v.add(task);
      }
      return v;
    });

    // must run execute outside of lock
    if (startCapture[0]) {
      executor.execute(worker);
    }
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Task to be executed
   * @return Future to represent when the execution has occurred
   */
  public ListenableFuture<?> submit(Object threadKey, Runnable task) {
    return submit(threadKey, task, null);
  }
  
  /**
   * Submit a task to be run with a given thread key.
   * 
   * @param <T> type of result returned from the future
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Runnable to be executed
   * @param result Result to be returned from future when task completes
   * @return Future to represent when the execution has occurred and provide the given result
   */
  public <T> ListenableFuture<T> submit(Object threadKey, Runnable task, T result) {
    return submit(threadKey, RunnableCallableAdapter.adapt(task, result));
  }
  
  /**
   * Submit a callable to be run with a given thread key.
   * 
   * @param <T> type of result returned from the future
   * @param threadKey object key where {@code equals()} will be used to determine execution thread
   * @param task Callable to be executed
   * @return Future to represent when the execution has occurred and provide the result from the callable
   */
  public <T> ListenableFuture<T> submit(Object threadKey, Callable<T> task) {
    ArgumentVerifier.assertNotNull(threadKey, "threadKey");
    ArgumentVerifier.assertNotNull(task, "task");
    
    ListenableRunnableFuture<T> rf = new ListenableFutureTask<>(false, task);
    
    addTask(threadKey, rf, executor);
    
    return rf;
  }
  
  /**
   * Worker which will consume through a given queue of tasks.  Each key is represented by one 
   * worker at any given time.
   * 
   * @since 1.0.0
   */
  protected class TaskQueueWorker implements Runnable {
    protected final Object mapKey;
    // we treat the first task special to attempt to avoid constructing the ArrayDeque
    protected volatile Runnable firstTask;
    protected Queue<Runnable> queue;  // locked around workerLock
    
    protected TaskQueueWorker(Object mapKey, Runnable firstTask) {
      this.mapKey = mapKey;
      this.queue = null;
      this.firstTask = firstTask;
    }
    
    /**
     * Call to get this workers current queue size.
     * 
     * @return How many tasks are waiting to be executed.
     */
    public int getQueueSize() {
      // the default implementation is very inaccurate
      int[] resultCapture = new int[1];
      taskWorkers.computeIfPresent(mapKey, (k, v) -> {
        resultCapture[0] = (v.firstTask == null ? 0 : 1) + (v.queue == null ? 0 : v.queue.size());
        return v;
      });
      return resultCapture[0];
    }
    
    /**
     * You MUST be within `compute` of taskWorkers before calling into this.  This is designed to 
     * be overridden if you need to track how tasks are being added.
     * 
     * @param task Runnable to add to the worker's queue
     */
    protected void add(Runnable task) {
      if (queue == null) {
        queue = new ArrayDeque<>(ARRAY_DEQUE_INITIAL_SIZE);
      }
      queue.add(task);
    }
    
    /**
     * Runs the provided task in the invoking thread.  This is designed to be overridden if 
     * needed.  No exceptions will ever be thrown from this call.
     * 
     * @param task Runnable to run
     */
    protected void runTask(Runnable task) {
      try {
        task.run();
      } catch (Throwable t) {
        ExceptionUtils.handleException(t);
      }
    }
    
    @Override
    public void run() {
      int consumedItems = 0;
      // firstTask may be null if we exceeded our maxTasksPerCycle
      if (firstTask != null) {
        consumedItems++;
        // we need to set firstTask to null before we run the task (for semi-accurate queue size)
        Runnable task = firstTask;
        // set to null to allow GC
        firstTask = null;
        
        runTask(task);
      }
      
      while (true) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Queue<Runnable>[] nextQueue = new Queue[1];
        final int fConsumedItems = consumedItems;
        TaskQueueWorker self = taskWorkers.compute(mapKey, (k, v) -> {
          if (queue == null) {  // nothing left to run
            return null;
          } else if (fConsumedItems < maxTasksPerCycle) {
            // we can run at least one task...let's figure out how much we can run
            if (queue.size() + fConsumedItems <= maxTasksPerCycle) {
              // we can run the entire next queue
              nextQueue[0] = queue;
              queue = null;
            } else {
              // we need to run a subset of the queue, so copy and remove what we can run
              int nextListSize = maxTasksPerCycle - fConsumedItems;
              nextQueue[0] = new ArrayDeque<>(nextListSize);
              Iterator<Runnable> it = queue.iterator();
              do {
                nextQueue[0].add(it.next());
                it.remove();
              } while (nextQueue[0].size() < nextListSize);
            }
          } else {
            // just don't set the queue, but also don't remove the worker.  This will 
            // re-execute this worker to give other workers a chance to run
            nextQueue[0] = null;
          }
          return v;
        });

        if (nextQueue[0] != null) {
          consumedItems += nextQueue[0].size();
          for (Runnable r : nextQueue[0]) {
            runTask(r);
          }
        } else {
          if (self != null) {
            // we are just re-executing ourselves (reached execution limit)
            executor.execute(this);
          }
          break;
        }
      }
    }
  }
  
  /**
   * Extending class that will accurately track how many tasks have been added, and how many 
   * have been run.  Thus providing an accurate queue size statistic.
   * 
   * @since 1.2.0
   */
  protected class StatisticWorker extends TaskQueueWorker {
    private final LongAdder queueSize;
    
    protected StatisticWorker(Object mapKey, Runnable firstTask) {
      super(mapKey, firstTask);
      
      queueSize = new LongAdder();
      queueSize.increment();
    }
    
    @Override
    public int getQueueSize() {
      return queueSize.intValue();
    }
    
    @Override
    protected void add(Runnable task) {
      queueSize.increment();
      
      super.add(task);
    }
    
    @Override
    protected void runTask(Runnable task) {
      queueSize.decrement();
      
      super.runTask(task);
    }
  }
  
  /**
   * Simple {@link SubmitterExecutor} implementation that submits for a given key.
   * 
   * @since 2.5.0
   */
  protected class KeySubmitter implements SubmitterExecutor {
    protected final Object threadKey;
    
    protected KeySubmitter(Object threadKey) {
      this.threadKey = threadKey;
    }
    
    @Override
    public void execute(Runnable command) {
      KeyDistributedExecutor.this.execute(threadKey, command);
    }

    @Override
    public ListenableFuture<?> submit(Runnable task) {
      return KeyDistributedExecutor.this.submit(threadKey, task);
    }

    @Override
    public <T> ListenableFuture<T> submit(Runnable task, T result) {
      return KeyDistributedExecutor.this.submit(threadKey, task, result);
    }

    @Override
    public <T> ListenableFuture<T> submit(Callable<T> task) {
      return KeyDistributedExecutor.this.submit(threadKey, task);
    }
  }
}
