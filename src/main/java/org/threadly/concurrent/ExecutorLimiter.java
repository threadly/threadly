package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

/**
 * This class is designed to limit how much parallel execution happens 
 * on a provided {@link Executor}.  This allows the user to have one 
 * thread pool for all their code, and if they want certain sections 
 * to have less levels of parallelism (possibly because those those 
 * sections would completely consume the global pool), they can wrap 
 * the executor in this class.
 * 
 * Thus providing you better control on the absolute thread count and 
 * how much parallism can occur in different sections of the program.  
 * 
 * Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.
 * 
 * @author jent - Mike Jensen
 */
public class ExecutorLimiter extends AbstractThreadPoolLimiter 
                             implements Executor {
  private final Executor executor;
  private final Queue<Runnable> waitingTasks;
  
  /**
   * Construct a new execution limiter that implements the 
   * {@link Executor} interface.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    super(maxConcurrency);
    
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
        executor.execute(new RunnableWrapper(waitingTasks.poll()));
      }
    }
  }

  @Override
  public void execute(Runnable command) {
    if (canRunTask()) {  // try to avoid adding to queue if we can
      executor.execute(new RunnableWrapper(command));
    } else {
      waitingTasks.add(command);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }
  
  /**
   * Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class RunnableWrapper extends VirtualRunnable {
    private final Runnable runnable;
    
    public RunnableWrapper(Runnable runnable) {
      this.runnable = runnable;
    }
    
    @Override
    public void run() {
      try {
        if (factory != null && 
            runnable instanceof VirtualRunnable) {
          VirtualRunnable vr = (VirtualRunnable)runnable;
          vr.run(factory);
        } else {
          runnable.run();
        }
      } finally {
        handleTaskFinished();
      }
    }
  }
}
