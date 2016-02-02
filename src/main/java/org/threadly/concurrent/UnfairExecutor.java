package org.threadly.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

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
  private final SingleThreadScheduler[] schedulers;
  private volatile boolean  allThreadsStarted;  // only transitions to true once
  
  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.  This defaults to using 
   * daemon threads.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   */
  public UnfairExecutor(int threadCount) {
    this(threadCount, true);
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param useDaemonThreads {@code true} if created threads should be daemon
   */
  public UnfairExecutor(int threadCount, boolean useDaemonThreads) {
    this(threadCount, new ConfigurableThreadFactory(UnfairExecutor.class.getSimpleName() + "-", true, 
                                                    useDaemonThreads, Thread.NORM_PRIORITY, null, null));
  }

  /**
   * Constructs a new {@link UnfairExecutor} with a provided thread count and factory.
   * 
   * @param threadCount Number of threads, recommended to be a prime number
   * @param threadFactory thread factory for producing new threads within executor
   */
  public UnfairExecutor(int threadCount, ThreadFactory threadFactory) {
    ArgumentVerifier.assertGreaterThanZero(threadCount, "threadCount");
    
    schedulers = new SingleThreadScheduler[threadCount];
    for (int i = 0; i < threadCount; i++) {
      schedulers[i] = new SingleThreadScheduler(threadFactory);
    }
    allThreadsStarted = false;
  }
  
  @Override
  protected void doExecute(Runnable task) {
    long seed = Math.abs(task.hashCode() ^ Clock.lastKnownTimeNanos());
    SingleThreadScheduler scheduler1 = schedulers[(int)(seed % schedulers.length)];
    NoThreadScheduler nts1 = scheduler1.sManager.scheduler;
    if (nts1.getActiveTaskCount() == 0) {
      scheduler1.execute(task);
    } else {
      SingleThreadScheduler scheduler2 = schedulers[(int)((seed + 1) % schedulers.length)];
      NoThreadScheduler nts2 = scheduler2.sManager.scheduler;
      if (nts1.highPriorityQueueSet.executeQueue.size() < nts2.highPriorityQueueSet.executeQueue.size()) {
        scheduler1.execute(task);
      } else {
        scheduler2.execute(task);
      }
    }
  }
  
  /**
   * Getter for the currently set max thread pool size.
   * 
   * @return current max pool size
   */
  public int getMaxPoolSize() {
    return schedulers.length;
  }

  /**
   * Getter for the current quantity of threads running in this pool (either active or idle).  
   * This is different than the size returned from {@link #getMaxPoolSize()} in that we 
   * lazily create threads.  This represents the amount of threads needed to be created so far, 
   * where {@link #getMaxPoolSize()} represents the amount of threads the pool may grow to.  
   * 
   * Unlike other pools, this call may have O(n threads) to calculate the result.
   * 
   * @return current thread count
   */
  public int getCurrentPoolSize() {
    if (allThreadsStarted && ! isShutdown()) {
      // shortcut to avoid looping over all threads
      return schedulers.length;
    }
    
    int result = 0;
    for (SingleThreadScheduler sts : schedulers) {
      if (sts.sManager.execThread.isAlive()) {
        result++;
      }
    }
    if (result == schedulers.length) {
      allThreadsStarted = true;
    }
    return result;
  }
  
  /**
   * Ensures all threads have been started, it will create threads till the thread count matches 
   * the set pool size (checked via {@link #getMaxPoolSize()}).  These new threads will remain 
   * idle till there is tasks ready to execute.
   */
  public void prestartAllThreads() {
    if (allThreadsStarted) {
      // shortcut to avoid looping over all threads
      return;
    }
    
    for (SingleThreadScheduler sts : schedulers) {
      if (! sts.sManager.execThread.isAlive()) {
        // easiest way to start it up
        try {
          sts.execute(DoNothingRunnable.instance());
        } catch (RejectedExecutionException e) {
          // shutting down
          return;
        }
      }
    }
    
    allThreadsStarted = true;
  }

  /**
   * Function to check if the thread pool is currently accepting and handling tasks.
   * 
   * @return {@code true} if thread pool is running
   */
  public boolean isShutdown() {
    return schedulers[0].isShutdown();
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
    synchronized (schedulers) {
      if (isShutdown()) {
        return;
      }
      
      for (SingleThreadScheduler sts : schedulers) {
        sts.shutdown();
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
    synchronized (schedulers) {
      if (isShutdown()) {
        return Collections.emptyList();
      }
      
      List<Runnable> result = new ArrayList<Runnable>();
      for (SingleThreadScheduler sts : schedulers) {
        result.addAll(sts.shutdownNow());
      }
      return result;
    }
  }

  /**
   * Block until the thread pool has shutdown and all threads have been stopped.  If neither 
   * {@link #shutdown()} or {@link #shutdownNow()} is invoked, then this will block forever.
   * 
   * @throws InterruptedException Thrown if blocking thread is interrupted waiting for shutdown
   */
  public void awaitTermination() throws InterruptedException {
    for (SingleThreadScheduler sts : schedulers) {
      sts.awaitTermination();
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
    for (SingleThreadScheduler sts : schedulers) {
      long remainingWait = timeoutMillis - (Clock.lastKnownForwardProgressingMillis() - startTime);
      if (remainingWait <= 0 || ! sts.awaitTermination(remainingWait)) {
        return false;
      }
    }
    return true;
  }
}
