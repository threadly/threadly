package org.threadly.concurrent.limiter;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.threadly.concurrent.SubmitterExecutorInterface;
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
 * @since 1.0.0
 */
public class ExecutorLimiter extends AbstractThreadPoolLimiter 
                             implements SubmitterExecutorInterface {
  protected final Executor executor;
  protected final Queue<LimiterRunnableWrapper> waitingTasks;
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    this(executor, maxConcurrency, null);
  }
  
  /**
   * Construct a new execution limiter that implements the {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum quantity of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool ({@code null} to not change thread names)
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency, String subPoolName) {
    super(maxConcurrency, subPoolName);

    ArgumentVerifier.assertNotNull(executor, "executor");
    
    this.executor = executor;
    waitingTasks = new ConcurrentLinkedQueue<LimiterRunnableWrapper>();
  }
  
  @Override
  protected void consumeAvailable() {
    /* must synchronize in queue consumer to avoid 
     * multiple threads from consuming tasks in parallel 
     * and possibly emptying after .isEmpty() check but 
     * before .poll()
     */
    synchronized (this) {
      while (! waitingTasks.isEmpty() && canRunTask()) {
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
   * Executes the wrapper if there is room in the limiter, otherwise it will queue the wrapper to 
   * be executed once the limiter has room.
   * 
   * @param lrw Wrapper that is ready to execute once there is available slots in the limiter
   */
  protected void executeWrapper(LimiterRunnableWrapper lrw) {
    if (waitingTasks.isEmpty() && canRunTask()) {
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
}
