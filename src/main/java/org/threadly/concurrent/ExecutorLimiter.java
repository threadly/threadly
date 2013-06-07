package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is designed to limit how much parallel execution happens 
 * on a provided executor.  This allows the user to have one thread pool 
 * for all their code, and if they want certain sections to have less 
 * levels of parallelism (possibly because those those sections would 
 * completely consume the global pool), they can wrap the executor in 
 * this class.
 * 
 * Thus providing you better control on the absolute thread count and 
 * how much parallism can occur in different sections of the program.  
 * 
 * Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.
 * 
 * @author jent - Mike Jensen
 */
public class ExecutorLimiter implements Executor {
  private final Executor executor;
  private final int maxConcurrency;
  private final AtomicInteger currentlyRunning;
  private final Queue<Runnable> waitingTasks;
  
  /**
   * Construct a new execution limiter.
   * 
   * @param executor {@link Executor} to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public ExecutorLimiter(Executor executor, int maxConcurrency) {
    if (executor == null) {
      throw new IllegalArgumentException("Must provide executor");
    } else if (maxConcurrency < 1) {
      throw new IllegalArgumentException("max concurrency must be at least 1");
    }
    
    this.executor = executor;
    this.maxConcurrency = maxConcurrency;
    currentlyRunning = new AtomicInteger(0);
    waitingTasks = new ConcurrentLinkedQueue<Runnable>();
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
   * Is block to verify a task can run in a thread safe way.  
   * If this returns true currentlyRunning has been incremented and 
   * it expects the task to run and call handleTaskFinished 
   * when completed.
   * 
   * @return returns true if the task can run
   */
  protected boolean canRunTask() {
    while (true) {  // loop till we have a result
      int currentValue = currentlyRunning.get();
      if (currentValue < maxConcurrency) {
        if (currentlyRunning.compareAndSet(currentValue, 
                                           currentValue + 1)) {
          return true;
        } // else retry in while loop
      } else {
        return false;
      }
    }
  }
  
  /**
   * Will run as many waiting tasks as it can.
   */
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
  
  /**
   * Should be called after every task completes.  This decrements 
   * currentlyRunning in a thread safe way, then will run any waiting 
   * tasks which exists.
   */
  protected void handleTaskFinished() {
    boolean reducedTaskCount = false;
    while (! reducedTaskCount) {
      int currentValue = currentlyRunning.get();
      reducedTaskCount = currentlyRunning.compareAndSet(currentValue, 
                                                        currentValue - 1);
    }
    
    consumeAvailable(); // allow any waiting tasks to run
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
