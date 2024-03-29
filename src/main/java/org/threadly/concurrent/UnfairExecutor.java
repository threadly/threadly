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
 * A very high performance {@link SubmitterExecutor} implementation.  Though to get those 
 * performance gains, some guarantees are reduced.  Most prominently is execution order, this 
 * scheduler does not ensure that tasks are executed in the order they are submitted, but rather 
 * tasks are consumed however is fastest.
 * <p>
 * This executor has an execution queue per thread.  That way each thread has a many producer, one 
 * consumer safety guarantees.  Compared to other pools which have to deal with a many to many 
 * thread safety issue.  Determining the thread queue may be based on a variety of information.  A 
 * couple built in distribution solutions are {@link TaskHashXorTimeStripeGenerator} (default) and 
 * {@link AtomicStripeGenerator}.
 * <p>
 * This scheduler will work best when the following conditions are true.  First because a long 
 * running task can block other tasks from running (even when other threads are idle).  It is best 
 * that tasks should be equally sized.  We also recommend having thread counts which are prime 
 * numbers for a more even hash distribution.  It is also important to recognize that it is 
 * generally a bad idea to block any of these threads waiting for results from processing that is 
 * expected to happen on the same executor.
 * 
 * @since 4.5.0
 */
public class UnfairExecutor extends AbstractSubmitterExecutor {
  /**
   * Strategy for taking in a task and producing a long which will be translated to which thread 
   * the task should be distributed on to.  This number is only a guide for the scheduler, the 
   * scheduler may choose another thread depending on what internal balancing may be possible.
   * 
   * @since 4.5.0
   */
  public interface TaskStripeGenerator {
    /**
     * Generate an identifier for the stripe to distribute the task on to.
     * 
     * @param task Task which can be used for referencing in determining the stripe
     * @return Any positive or negative long value to represent the stripe
     */
    public long getStripe(Runnable task);
  }
  
  /**
   * Generator which will determine the task stripe by using the identity hash of the runnable and 
   * {@link Clock#lastKnownTimeNanos()}.  This is the fastest built in option, however submissions 
   * of the same task many times without the clock being updated can result in a single thread 
   * being unfairly burdened.  Because of that it is highly recommended to over-size your pool if 
   * you are using this distributor.
   * <p>
   * A possibly more fair, but slower stripe generator would be {@link AtomicStripeGenerator}.  
   * <p>
   * This class should not be constructed, instead it should be provided via the static function 
   * {@link TaskHashXorTimeStripeGenerator#instance()}.
   * 
   * @since 4.5.0
   */
  public static class TaskHashXorTimeStripeGenerator implements TaskStripeGenerator {
    private static final TaskHashXorTimeStripeGenerator INSTANCE = 
        new TaskHashXorTimeStripeGenerator();
    
    /**
     * Provides an instance which can be provided into the constructor of {@link UnfairExecutor}.
     * 
     * @return TaskHashXorTimeStripeGenerator instance
     */
    public static TaskHashXorTimeStripeGenerator instance() {
      return INSTANCE;
    }
    
    private TaskHashXorTimeStripeGenerator() {
      // don't allow external construction
    }
    
    @Override
    public long getStripe(Runnable task) {
      return System.identityHashCode(task) ^ Clock.lastKnownTimeNanos();
    }
  }

  /**
   * Stripe generator which will round robin distribute tasks to threads.  Internally this uses a 
   * {@link AtomicLong}, which means if lots of tasks are being submitted in parallel there can be 
   * a lot of compare and swap overhead compared to {@link TaskHashXorTimeStripeGenerator}.  
   * <p>
   * This class should not be constructed, instead it should be provided via the static function 
   * {@link AtomicStripeGenerator#instance()}.
   * 
   * @since 4.5.0
   */
  public static class AtomicStripeGenerator implements TaskStripeGenerator {
    /**
     * Provides an instance which can be provided into the constructor of {@link UnfairExecutor}.
     * 
     * @return A new AtomicStripeGenerator instance
     */
    public static AtomicStripeGenerator instance() {
      return new AtomicStripeGenerator();
    }
    
    private final AtomicLong stripe;
    
    private AtomicStripeGenerator() {
      // don't allow external construction
      stripe = new AtomicLong();
    }
    
    @Override
    public long getStripe(Runnable task) {
      return stripe.getAndIncrement();
    }
  }
  
  protected final Worker[] schedulers;
  private final AtomicBoolean shutdownStarted;
  private final TaskStripeGenerator stripeGenerator;
  
  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This defaults to using 
   * daemon threads.  This also defaults to using the {@link TaskHashXorTimeStripeGenerator}.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   */
  public UnfairExecutor(int threadCount) {
    this(threadCount, true, TaskHashXorTimeStripeGenerator.instance());
  }
  
  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This defaults to using 
   * daemon threads.  
   * <p>
   * Possible built in stripe generators for use would be {@link AtomicStripeGenerator} or 
   * {@link TaskHashXorTimeStripeGenerator}.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param stripeGenerator Generator for figuring out how a task is assigned to a thread
   */
  public UnfairExecutor(int threadCount, TaskStripeGenerator stripeGenerator) {
    this(threadCount, true, stripeGenerator);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.    This also defaults 
   * to using the {@link TaskHashXorTimeStripeGenerator}.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param useDaemonThreads {@code true} if created threads should be daemon
   */
  public UnfairExecutor(int threadCount, boolean useDaemonThreads) {
    this(threadCount, useDaemonThreads, TaskHashXorTimeStripeGenerator.instance());
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  
   * <p>
   * Possible built in stripe generators for use would be {@link AtomicStripeGenerator} or 
   * {@link TaskHashXorTimeStripeGenerator}.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param useDaemonThreads {@code true} if created threads should be daemon
   * @param stripeGenerator Generator for figuring out how a task is assigned to a thread
   */
  public UnfairExecutor(int threadCount, boolean useDaemonThreads, 
                        TaskStripeGenerator stripeGenerator) {
    this(threadCount, 
         new ConfigurableThreadFactory(UnfairExecutor.class.getSimpleName() + "-", true, 
                                       useDaemonThreads, Thread.NORM_PRIORITY, null, null, null), 
         stripeGenerator);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count and factory.  This also 
   * defaults to using the {@link TaskHashXorTimeStripeGenerator}.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param threadFactory thread factory for producing new threads within executor
   */
  public UnfairExecutor(int threadCount, ThreadFactory threadFactory) {
    this(threadCount, threadFactory, TaskHashXorTimeStripeGenerator.instance());
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count and factory.  
   * <p>
   * Possible built in stripe generators for use would be {@link AtomicStripeGenerator} or 
   * {@link TaskHashXorTimeStripeGenerator}.
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
    final Worker firstWorker = schedulers[0];
    // let first worker start all the other threads as soon as possible
    firstWorker.addTask(new Runnable() {
      @Override
      public void run() {
        for (Worker w : schedulers) {
          if (w != firstWorker) {
            w.start();
          }
        }
      }
    });
    firstWorker.start();
  }
  
  @Override
  protected void doExecute(Runnable task) {
    if (shutdownStarted.get()) {
      throw new RejectedExecutionException("Pool is shutdown");
    }
    
    schedulers[Math.floorMod(stripeGenerator.getStripe(task), schedulers.length)].addTask(task);
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
   * for the shutdown of the scheduler to finish.  If {@link #shutdown()} or 
   * {@link #shutdownNow()} has already been called, this will have no effect.  
   * <p>
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
    
    List<Runnable> result = new ArrayList<>();
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
      if (remainingWait > 0) {
        w.thread.join(remainingWait);
      }
      if (w.thread.isAlive()) {
        return false;
      }
    }
    return true;
  }
  
  @Override
  protected void finalize() throws Throwable {
    shutdown();
  }
  
  /**
   * Worker task for executing tasks on the provided thread.  This worker maintains an internal 
   * queue for which tasks can be added on.  It will park itself once idle, and resume if tasks 
   * are later then added.
   * 
   * @since 4.5.0
   */
  protected static class Worker extends AbstractService implements Runnable {
    protected final Thread thread;
    protected final Queue<Runnable> taskQueue;
    private volatile boolean parked;
    private Worker checkNeighborWorker;
    private Worker wakupNeighborWorker;
    
    public Worker(ThreadFactory threadFactory) {
      thread = threadFactory.newThread(this);
      if (thread.isAlive()) {
        throw new IllegalThreadStateException();
      }
      taskQueue = new ConcurrentLinkedQueue<>();
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
          try {
            task.run();
          } catch (Throwable t) {
            ExceptionUtils.handleException(t);
          }
        } else if (! parked) {
          // check neighbor worker to see if they need help
          task = checkNeighborWorker.taskQueue.poll();
          if (task != null) {
            try {
              task.run();
            } catch (Throwable t) {
              ExceptionUtils.handleException(t);
            }
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
   * Task for shutting down worker thread.  Used in {@link #shutdown()}.
   * 
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
}
