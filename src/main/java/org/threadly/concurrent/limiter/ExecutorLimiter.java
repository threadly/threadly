package org.threadly.concurrent.limiter;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.threadly.concurrent.SubmitterExecutorInterface;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;

/**
 * <p>This class is designed to limit how much parallel execution happens 
 * on a provided {@link Executor}.  This allows the user to have one 
 * thread pool for all their code, and if they want certain sections 
 * to have less levels of parallelism (possibly because those those 
 * sections would completely consume the global pool), they can wrap 
 * the executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and 
 * how much parallelism can occur in different sections of the program.</p>  
 * 
 * <p>Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.</p>
 * 
 * @author jent - Mike Jensen
 */
public class ExecutorLimiter extends AbstractThreadPoolLimiter 
                             implements SubmitterExecutorInterface {
  protected final Executor executor;
  protected final Queue<Runnable> waitingTasks;
  
  /**
   * Construct a new execution limiter that implements the 
   * {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    this(executor, maxConcurrency, null);
  }
  
  /**
   * Construct a new execution limiter that implements the 
   * {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool (null to not change thread names)
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency, String subPoolName) {
    super(maxConcurrency, subPoolName);
    
    if (executor == null) {
      throw new IllegalArgumentException("Must provide executor");
    }
    
    this.executor = executor;
    waitingTasks = new ConcurrentLinkedQueue<Runnable>();
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
        executor.execute(new LimiterRunnableWrapper(waitingTasks.poll()));
      }
    }
  }

  @Override
  public void execute(Runnable task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide runnable");
    }
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      executor.execute(new LimiterRunnableWrapper(task));
    } else {
      waitingTasks.add(task);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    execute(lft);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    execute(lft);
    
    return lft;
  }
}