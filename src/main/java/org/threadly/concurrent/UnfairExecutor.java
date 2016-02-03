package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.threadly.util.AbstractService;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;
import org.threadly.util.ExceptionUtils;

/**
 * <p>A very high performance {@link SubmitterExecutor} implementation.  Though to get those 
 * performance gains, some guarantees are reduced.  Most prominently is execution order, this 
 * scheduler does not ensure that tasks are executed in the order they are submitted, but rather 
 * tasks are consumed however is fastest.</p>
 * 
 * <p>This executor has an execution queue per thread.  That way each thread has a many producer, 
 * one consumer safety guarantees.  Vs normal pools which have to deal with a many to many thread 
 * safety issue.  Determining the thread queue may be based on a variety of information, some 
 * examples are the {@link Object#hashCode()} of the provided task, the current time, or other 
 * weak choices to try and distribute work evenly.</p>
 * 
 * <p>In this it is obviously possible for one long running task to block other tasks needing to 
 * run (even if other threads are idle).  Because of that tasks should be equally sized, having 
 * large tasks mixed in with small ones will make this more problematic.  We also recommend having 
 * thread counts which are prime numbers for a more even hash distribution.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
public class UnfairExecutor extends AbstractSubmitterExecutor {
  /**
   * Generator which will determine the task stripe by using the runnables hash code and 
   * {@link Clock#lastKnownTimeNanos()}.  This is the fastest built in option (and thus why it is 
   * the default).  However submissions of the same task many times without the clock being updated 
   * can result in a scheduler being unfairly burdened.
   */
  public static final TaskStripeGenerator TASK_STRIPE_HASH_AND_LAST_KNOWN_TIME = new TaskStripeGenerator() {
    @Override
    public long getStripe(Runnable task) {
      return task.hashCode() ^ Clock.lastKnownTimeNanos();
    }
  };
  /**
   * Generator which will round robin distribute tasks to threads.  Internally this uses a 
   * {@link AtomicLong}, which means if lots of tasks are being submitted in parallel there can be 
   * a lot of compare and swap overhead compared to {@link #TASK_STRIPE_HASH_AND_LAST_KNOWN_TIME}.
   */
  public static final TaskStripeGenerator TASK_STRIPE_ATOMIC_INCREMENTING = new TaskStripeGenerator() {
    private final AtomicLong stripe = new AtomicLong();
    
    @Override
    public long getStripe(Runnable task) {
      return stripe.getAndIncrement();
    }
  };
  
  private final Worker[] schedulers;
  private final AtomicBoolean shutdownStarted;
  private final TaskStripeGenerator stripeGenerator;
  
  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This defaults to using 
   * daemon threads.  This also defaults to determining a thread by using the runnable hash code and 
   * the last known time.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   */
  public UnfairExecutor(int threadCount) {
    this(threadCount, true, TASK_STRIPE_HASH_AND_LAST_KNOWN_TIME);
  }
  
  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This defaults to using 
   * daemon threads.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param stripeGenerator Generator for figuring out how a task is assigned to a thread
   */
  public UnfairExecutor(int threadCount, TaskStripeGenerator stripeGenerator) {
    this(threadCount, true, stripeGenerator);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This also defaults to 
   * determining a thread by using the runnable hash code and the last known time.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param useDaemonThreads {@code true} if created threads should be daemon
   */
  public UnfairExecutor(int threadCount, boolean useDaemonThreads) {
    this(threadCount, useDaemonThreads, TASK_STRIPE_HASH_AND_LAST_KNOWN_TIME);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param useDaemonThreads {@code true} if created threads should be daemon
   * @param stripeGenerator Generator for figuring out how a task is assigned to a thread
   */
  public UnfairExecutor(int threadCount, boolean useDaemonThreads, 
                        TaskStripeGenerator stripeGenerator) {
    this(threadCount, 
         new ConfigurableThreadFactory(UnfairExecutor.class.getSimpleName() + "-", true, 
                                       useDaemonThreads, Thread.NORM_PRIORITY, null, null), 
         stripeGenerator);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count and factory.  This also 
   * defaults to determining a thread by using the runnable hash code and the last known time.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param threadFactory thread factory for producing new threads within executor
   */
  public UnfairExecutor(int threadCount, ThreadFactory threadFactory) {
    this(threadCount, threadFactory, TASK_STRIPE_HASH_AND_LAST_KNOWN_TIME);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count and factory.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param threadFactory thread factory for producing new threads within executor
   * @param stripeGenerator Generator for figuring out how a task is assigned to a thread
   */
  public UnfairExecutor(int threadCount, ThreadFactory threadFactory, 
                        TaskStripeGenerator stripeGenerator) {
    ArgumentVerifier.assertGreaterThanZero(threadCount, "threadCount");
    ArgumentVerifier.assertNotNull(stripeGenerator, "stripeGenerator");
    
    this.schedulers = new Worker[threadCount];
    this.shutdownStarted = new AtomicBoolean(false);
    this.stripeGenerator = stripeGenerator;
    
    for (int i = 0; i < threadCount; i++) {
      schedulers[i] = new Worker(threadFactory);
      if (i > 0) {
        schedulers[i].setNeighborWorker(schedulers[i - 1]);
      }
    }
    schedulers[0].setNeighborWorker(schedulers[schedulers.length - 1]);
    // can only start once full neighbor chain is established
    for (Worker w : schedulers) {
      w.start();
    }
  }
  
  @Override
  protected void doExecute(Runnable task) {
    if (shutdownStarted.get()) {
      throw new RejectedExecutionException("Pool is shutdown");
    }
    
    schedulers[(int)(Math.abs(stripeGenerator.getStripe(task)) % schedulers.length)].addTask(task);
  }

  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown() {
    return shutdownStarted.get();
  }

  /**
   * Stops any new tasks from being submitted to the pool.  But allows all tasks which are 
   * submitted to execute, or scheduled (and have elapsed their delay time) to run.  If recurring 
   * tasks are present they will also be unable to reschedule.  This call will not block to wait 
   * for the shutdown of the scheduler to finish.  If {@code shutdown()} or 
   * {@link #shutdownNow()} has already been called, this will have no effect.  
   * 
   * If you wish to not want to run any queued tasks you should use {@link #shutdownNow()}.
   */
  public void shutdown() {
    if (shutdownStarted.compareAndSet(false, true)) {
      for (Worker w : schedulers) {
        w.addTask(new ShutdownTask(w));
      }
    }
  }

  /**
   * Stops any new tasks from being submitted to the pool.  If any tasks are waiting for execution 
   * they will be prevented from being run.  If a task is currently running it will be allowed to 
   * finish (though this call will not block waiting for it to finish).
   * 
   * @return returns a list of runnables which were waiting in the queue to be run at time of shutdown
   */
  public List<Runnable> shutdownNow() {
    shutdownStarted.set(true);
    
    List<Runnable> result = new ArrayList<Runnable>();
    for (Worker w : schedulers) {
      w.stopIfRunning();
      Iterator<Runnable> it = w.taskQueue.iterator();
      while (it.hasNext()) {
        Runnable task = it.next();
        it.remove();
        if (! (task instanceof ShutdownTask)) {
          result.add(task);
        }
      }
    }
    return result;
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block forever.
   * 
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public void awaitTermination() throws InterruptedException {
    for (Worker w : schedulers) {
      w.thread.join();
    }
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block until the 
   * timeout is reached.
   * 
   * @param timeoutMillis time to block and wait for thread pool to shutdown
   * @return {@code true} if the pool has shutdown, false if timeout was reached
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public boolean awaitTermination(long timeoutMillis) throws InterruptedException {
    long startTime = Clock.accurateForwardProgressingMillis();
    for (Worker w : schedulers) {
      long remainingWait = timeoutMillis - (Clock.lastKnownForwardProgressingMillis() - startTime);
      if (remainingWait <= 0) {
        return false;
      }
      w.thread.join(remainingWait);
      if (w.thread.isAlive()) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  protected void finalize() throws Throwable {
    shutdown();
    super.finalize();
  }
  
  /**
   * <p>Worker task for executing tasks on the provided thread.  This worker maintains an internal 
   * queue for which tasks can be added on.  It will park itself once idle, and resume if tasks 
   * are later then added.</p>
   *  
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  private static class Worker extends AbstractService implements Runnable {
    protected final Thread thread;
    private final Queue<Runnable> taskQueue;
    private volatile boolean parked;
    private Worker checkNeighborWorker;
    private Worker wakupNeighborWorker;
    
    public Worker(ThreadFactory threadFactory) {
      thread = threadFactory.newThread(this);
      if (thread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      taskQueue = new ConcurrentLinkedQueue<Runnable>();
      parked = false;
    }
    
    /**
     * Must be invoked with a non-null worker before starting.
     * 
     * @param w Worker to assist if we are idle
     */
    protected void setNeighborWorker(Worker w) {
      checkNeighborWorker = w;
      w.wakupNeighborWorker = this;
    }

    @Override
    protected void startupService() {
      if (checkNeighborWorker == null || wakupNeighborWorker == null) {
        throw new IllegalStateException();
      }
      thread.start();
    }

    @Override
    protected void shutdownService() {
      LockSupport.unpark(thread);
    }
    
    public void addTask(Runnable task) {
      taskQueue.add(task);
      if (parked) {
        parked = false;
        LockSupport.unpark(thread);
      } else if (wakupNeighborWorker.parked) {
        wakupNeighborWorker.parked = false;
        LockSupport.unpark(wakupNeighborWorker.thread);
      }
    }
    
    @Override
    public void run() {
      while (isRunning()) {
        Runnable task = taskQueue.poll();
        // just reset status, we should only shutdown by having the service stopped
        Thread.interrupted();
        if (task != null) {
          if (parked) {
            parked = false;
          }
          ExceptionUtils.runRunnable(task);
        } else if (! parked) {
          // check neighbor worker to see if they need help
          task = checkNeighborWorker.taskQueue.poll();
          if (task != null) {
            ExceptionUtils.runRunnable(task);
          } else {
            parked = true;
          }
        } else {
          LockSupport.park();
        }
      }
    }
  }
  
  /**
   * <p>Task for shutting down worker thread.  Used in {@link #shutdown()}.</p>
   * 
   * @author jent
   * @since 4.5.0
   */
  private static class ShutdownTask implements Runnable {
    private final Worker w;
    
    public ShutdownTask(Worker w) {
      this.w = w;
    }
    
    @Override
    public void run() {
      w.stopIfRunning();
      w.taskQueue.clear();
    }
  }
  
  /**
   * Strategy for taking in a task and producing a thread identifier to be distributed on.
   * 
   * @author jent - Mike Jensen
   * @since 4.5.0
   */
  public interface TaskStripeGenerator {
    /**
     * Generate a stripe to distribute the task on to.
     * 
     * @param task Task which can be used for referencing in determining the stripe
     * @return Any positive or negative long value to represent the stripe
     */
    public long getStripe(Runnable task);
  }
}
