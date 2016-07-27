package org.threadly.concurrent.wrapper.limiter;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.concurrent.AbstractSubmitterExecutor;
import org.threadly.concurrent.RunnableContainer;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.util.ArgumentVerifier;

/**
 * <p>This class is designed to limit how much parallel execution happens on a provided 
 * {@link Executor}.  This allows the user to have one thread pool for all their code, and if they 
 * want certain sections to have less levels of parallelism (possibly because those those sections 
 * would completely consume the global pool), they can wrap the executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and how much parallelism can 
 * occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  By using this you also 
 * are able to accomplish more efficiently thread use than multiple thread pools would.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.6.0 (since 1.0.0 at org.threadly.concurrent.limiter)
 */
public class ExecutorLimiter extends AbstractSubmitterExecutor implements SubmitterExecutor {
  protected final Executor executor;
  protected final Queue<LimiterRunnableWrapper> waitingTasks;
  protected final int maxConcurrency;
  private final AtomicInteger currentlyRunning;
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    ArgumentVerifier.assertGreaterThanZero(maxConcurrency, "maxConcurrency");
    ArgumentVerifier.assertNotNull(executor, "executor");
    
    this.executor = executor;
    this.waitingTasks = new ConcurrentLinkedQueue<LimiterRunnableWrapper>();
    this.maxConcurrency = maxConcurrency;
    this.currentlyRunning = new AtomicInteger(0);
  }
  
  /**
   * Call to check what the maximum concurrency this limiter will allow.
   * 
   * @return maximum concurrent tasks to be run
   */
  public int getMaxConcurrency() {
    return maxConcurrency;
  }
  
  /**
   * Returns how many tasks are currently being "limited" and thus are in queue to run from this 
   * limiter.
   * 
   * @return Quantity of tasks queued in this limiter
   */
  public int getUnsubmittedTaskCount() {
    return waitingTasks.size();
  }
  
  /**
   * Thread safe verification that the pool has space remaining to accept additional tasks.
   * 
   * If this returns {@code true} {@code currentlyRunning} has been incremented and it expects the 
   * task to run will invoke {@link #handleTaskFinished()} when completed.
   * 
   * @return {@code true} if the task can be submitted to the pool
   */
  private boolean canSubmitTasksToPool() {
    while (true) {  // loop till we have a result
      int currentValue = currentlyRunning.get();
      if (currentValue < maxConcurrency) {
        if (currentlyRunning.compareAndSet(currentValue, currentValue + 1)) {
          return true;
        } // else retry in while loop
      } else {
        return false;
      }
    }
  }
  
  protected void consumeAvailable() {
    /* must synchronize in queue consumer to avoid 
     * multiple threads from consuming tasks in parallel 
     * and possibly emptying after .isEmpty() check but 
     * before .poll()
     */
    synchronized (this) {
      while (! waitingTasks.isEmpty() && canSubmitTasksToPool()) {
        // by entering loop we can now execute task
        LimiterRunnableWrapper lrw = waitingTasks.poll();
        lrw.submitToExecutor();
      }
    }
  }

  @Override
  protected void doExecute(Runnable task) {
    LimiterRunnableWrapper lrw = new LimiterRunnableWrapper(executor, task);
    executeWrapper(lrw);
  }
  
  /**
   * Check that not only are we able to submit tasks to the pool, but there are no tasks currently 
   * waiting to already be submitted.  If only {@link #canSubmitTasksToPool()} is checked, tasks 
   * may be able to cut in line with tasks that are already queued in the waiting queue.
   * 
   * @return true if the task can be submitted to the pool 
   */
  protected boolean canRunTask() {
    return waitingTasks.isEmpty() && canSubmitTasksToPool();
  }
  
  /**
   * Executes the wrapper if there is room in the limiter, otherwise it will queue the wrapper to 
   * be executed once the limiter has room.
   * 
   * @param lrw Wrapper that is ready to execute once there is available slots in the limiter
   */
  protected void executeWrapper(LimiterRunnableWrapper lrw) {
    if (canRunTask()) {
      lrw.submitToExecutor();
    } else {
      addToQueue(lrw);
    }
  }
  
  /**
   * Adds the wrapper to the queue.  After adding to the queue it will be verified that there is 
   * not available slots in the limiter to consume (it or others) from the queue.
   * 
   * It is expected that you already attempted to run the task (by calling {@link #canRunTask()} 
   * before deferring to this.
   * 
   * @param lrw {@link LimiterRunnableWrapper} to add to the queue
   */
  protected void addToQueue(LimiterRunnableWrapper lrw) {
    waitingTasks.add(lrw);
    consumeAvailable(); // call to consume in case task finished after first check
  }
  
  /**
   * <p>Generic wrapper for runnables which are used within the limiters.  This wrapper ensures 
   * that {@link #handleTaskFinished()} will be called after the task completes.</p>
   * 
   * @author jent - Mike Jensen
   * @since 1.0.0
   */
  protected class LimiterRunnableWrapper implements Runnable, RunnableContainer {
    protected final Executor executor;
    protected final Runnable runnable;
    
    public LimiterRunnableWrapper(Executor executor, Runnable runnable) {
      this.executor = executor;
      this.runnable = runnable;
    }
    
    /**
     * Called immediately after contained task finishes.  That way any additional cleanup needed 
     * can be run.
     */
    protected void doAfterRunTasks() {
      // nothing in the default implementation
    }
    
    /**
     * Submits this task to the executor.  This can be overridden if it needs to be submitted in a 
     * different way.
     */
    protected void submitToExecutor() {
      this.executor.execute(this);
    }
    
    @Override
    public void run() {
      try {
        runnable.run();
      } finally {
        try {
          doAfterRunTasks();
        } finally {
          currentlyRunning.decrementAndGet();
          
          consumeAvailable(); // allow any waiting tasks to run
        }
      }
    }

    @Override
    public Runnable getContainedRunnable() {
      return runnable;
    }
  }
}
