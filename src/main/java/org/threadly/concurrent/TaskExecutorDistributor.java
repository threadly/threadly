package org.threadly.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.concurrent.lock.VirtualLock;

/**
 * TaskDistributor is designed to take a multi threaded pool
 * and add tasks with a given key such that those tasks will
 * be run single threaded for any given key.  The thread which
 * runs those tasks may be different each time, but no two tasks
 * with the same key will ever be run in parallel.
 * 
 * Because of that, it is recommended that the executor provided 
 * has as many possible threads as possible keys that could be 
 * provided to be run in parallel.  If this class is starved for 
 * threads some keys may continue to process new tasks, while
 * other keys could be starved.
 * 
 * @author jent - Mike Jensen
 */
public class TaskExecutorDistributor {
  protected static final int DEFAULT_THREAD_KEEPALIVE_TIME = 1000 * 10;
  protected static final int DEFAULT_LOCK_PARALISM = 10;
  
  protected final Executor executor;
  protected final StripedLock sLock;
  private final Map<Object, TaskQueueWorker> taskWorkers;
  
  /**
   * Constructor which creates executor based off provided values.
   * 
   * @param expectedParallism Expected number of keys that will be used in parallel
   * @param maxThreadCount Max thread count (limits the qty of keys which are handled in parallel)
   */
  public TaskExecutorDistributor(int expectedParallism, int maxThreadCount) {
    this(new PriorityScheduledExecutor(expectedParallism, 
                                       maxThreadCount, 
                                       DEFAULT_THREAD_KEEPALIVE_TIME, 
                                       TaskPriority.High, 
                                       PriorityScheduledExecutor.DEFAULT_LOW_PRIORITY_MAX_WAIT), 
         new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * @param executor A multi-threaded executor to distribute tasks to.  
   *                 Ideally has as many possible threads as keys that 
   *                 will be used in parallel. 
   */
  public TaskExecutorDistributor(Executor executor) {
    this(DEFAULT_LOCK_PARALISM, executor);
  }
    
  /**
   * Constructor to use a provided executor implementation for running tasks.
   * 
   * @param expectedParallism level of expected qty of threads adding tasks in parallel
   * @param executor A multi-threaded executor to distribute tasks to.  
   *                 Ideally has as many possible threads as keys that 
   *                 will be used in parallel. 
   */
  public TaskExecutorDistributor(int expectedParallism, Executor executor) {
    this(executor, new StripedLock(expectedParallism, new NativeLockFactory()));
  }
  
  /**
   * used for testing, so that agentLock can be held and prevent execution.
   */
  protected TaskExecutorDistributor(Executor executor, StripedLock sLock) {
    if (executor == null) {
      throw new IllegalArgumentException("executor can not be null");
    }
    
    this.executor = executor;
    this.sLock = sLock;
    this.taskWorkers = new HashMap<Object, TaskQueueWorker>();
  }
  
  /**
   * @return executor tasks are being distributed to
   */
  public Executor getExecutor() {
    return executor;
  }
  
  /**
   * Provide a task to be run with a given thread key.
   * 
   * @param threadKey object key where hashCode will be used to determine execution thread
   * @param task Task to be executed.
   */
  public void addTask(Object threadKey, Runnable task) {
    if (threadKey == null) {
      throw new IllegalArgumentException("Must provide thread key");
    } else if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    VirtualLock agentLock = sLock.getLock(threadKey);
    synchronized (agentLock) {
      TaskQueueWorker worker = taskWorkers.get(threadKey);
      if (worker == null) {
        worker = new TaskQueueWorker(threadKey, agentLock);
        taskWorkers.put(threadKey, worker);
        worker.add(task);
        executor.execute(worker);
      } else {
        worker.add(task);
      }
    }
  }
  
  /**
   * Worker which will consume through a given queue of tasks.
   * Each key is represented by one worker at any given time.
   * 
   * @author jent - Mike Jensen
   */
  private class TaskQueueWorker implements Runnable {
    private final Object mapKey;
    private final VirtualLock agentLock;
    private LinkedList<Runnable> queue;
    
    private TaskQueueWorker(Object mapKey, 
                            VirtualLock agentLock) {
      this.mapKey = mapKey;
      this.agentLock = agentLock;
      this.queue = new LinkedList<Runnable>();
    }
    
    public void add(Runnable task) {
      synchronized (agentLock) {
        queue.addLast(task);
      }
    }
    
    private List<Runnable> next() {
      synchronized (agentLock) {
        List<Runnable> result = null;
        if (! queue.isEmpty()) {
          result = queue;
          queue = new LinkedList<Runnable>();
        }
        
        return result;
      }
    }
    
    @Override
    public void run() {
      while (true) {
        List<Runnable> nextList;
        synchronized (agentLock) {
          nextList = next();
          
          if (nextList == null) {
            taskWorkers.remove(mapKey);
            break;  // stop consuming tasks
          }
        }
        
        Iterator<Runnable> it = nextList.iterator();
        while (it.hasNext()) {
          try {
            it.next().run();
          } catch (Throwable t) {
            UncaughtExceptionHandler ueh = Thread.getDefaultUncaughtExceptionHandler();
            ueh.uncaughtException(Thread.currentThread(), t);
          }
        }
      }
    }
  }
}
