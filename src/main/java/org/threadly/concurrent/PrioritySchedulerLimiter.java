package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

/**
 * This class is designed to limit how much parallel execution happens 
 * on a provided {@link PriorityScheduledExecutor}.  This allows the 
 * implementor to have one thread pool for all their code, and if 
 * they want certain sections to have less levels of parallelism 
 * (possibly because those those sections would completely consume the 
 * global pool), they can wrap the executor in this class.
 * 
 * Thus providing you better control on the absolute thread count and 
 * how much parallism can occur in different sections of the program.  
 * 
 * Thus avoiding from having to create multiple thread pools, and also 
 * using threads more efficiently than multiple thread pools would.
 * 
 * @author jent - Mike Jensen
 */
public class PrioritySchedulerLimiter extends AbstractThreadPoolLimiter 
                                      implements PrioritySchedulerInterface {
  private final PrioritySchedulerInterface scheduler;
  private final Queue<PriorityRunnableWrapper> waitingTasks;
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerInterface}.
   * 
   * @param scheduler {@link PrioritySchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public PrioritySchedulerLimiter(PrioritySchedulerInterface scheduler, 
                                  int maxConcurrency) {
    super(maxConcurrency);
    
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler");
    }
    
    this.scheduler = scheduler;
    waitingTasks = new ConcurrentLinkedQueue<PriorityRunnableWrapper>();
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
        PriorityRunnableWrapper next = waitingTasks.poll();
        scheduler.execute(next, next.priority);
      }
    }
  }

  @Override
  public void execute(Runnable task) {
    execute(task, scheduler.getDefaultPriority());
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit(task, scheduler.getDefaultPriority());
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return submit(task, scheduler.getDefaultPriority());
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, 
             scheduler.getDefaultPriority());
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, delayInMs, 
                           scheduler.getDefaultPriority());
  }

  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs) {
    return submitScheduled(task, delayInMs, 
                           scheduler.getDefaultPriority());
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                           scheduler.getDefaultPriority());
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    PriorityRunnableWrapper wrapper = new PriorityRunnableWrapper(task, priority);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      scheduler.execute(wrapper, priority);
    } else {
      waitingTasks.add(wrapper);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public Future<?> submit(Runnable task, TaskPriority priority) {
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public <T> Future<T> submit(Callable<T> task, TaskPriority priority) {
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    scheduler.schedule(new DelayedExecution(task, priority), 
                       delayInMs, priority);
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs,
                                   TaskPriority priority) {
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs,
                                       TaskPriority priority) {
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }
  
  /**
   * Small runnable that allows scheduled tasks to pass through 
   * the same execution queue that immediate execution has to.
   * 
   * @author jent - Mike Jensen
   */
  protected class DelayedExecution extends VirtualRunnable {
    private final Runnable runnable;
    private final TaskPriority priority;
    
    public DelayedExecution(Runnable runnable, 
                            TaskPriority priority) {
      this.runnable = runnable;
      this.priority = priority;
    }
    
    @Override
    public void run() {
      execute(runnable, priority);
    }
  }

  /**
   * Wrapper for priority tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.
   * 
   * @author jent - Mike Jensen
   */
  protected class PriorityRunnableWrapper extends VirtualRunnable {
    private final Runnable runnable;
    private final TaskPriority priority;
    
    public PriorityRunnableWrapper(Runnable runnable, 
                                   TaskPriority priority) {
      this.runnable = runnable;
      this.priority = priority;
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
