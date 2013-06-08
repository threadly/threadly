package org.threadly.concurrent;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.util.ExceptionUtils;

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
        if (next.future == null) {
          scheduler.execute(next, next.priority);
        } else {
          next.future.setParentFuture(scheduler.submit(next, next.priority));
        }
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
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    PriorityRunnableWrapper wrapper = new PriorityRunnableWrapper(task, priority, null);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      scheduler.execute(wrapper, priority);
    } else {
      waitingTasks.add(wrapper);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public Future<?> submit(Runnable task, TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    FutureFuture<?> ff = new FutureFuture<Object>();
    
    submit(task, priority, ff);
    
    return ff;
  }
  
  private void submit(Runnable task, 
                      TaskPriority priority, FutureFuture<?> ff) {
    PriorityRunnableWrapper wrapper = new PriorityRunnableWrapper(task, priority, ff);
    
    if (canRunTask()) {  // try to avoid adding to queue if we can
      ff.setParentFuture(scheduler.submit(wrapper, priority));
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
  public <T> Future<T> submit(Callable<T> task, TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    }
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    throw new UnsupportedOperationException("Not implemented for limiter");  // TODO implement
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    scheduler.schedule(new DelayedExecution(task, priority, null), 
                       delayInMs, priority);
  }

  @Override
  public Future<?> submitScheduled(Runnable task, long delayInMs,
                                   TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }

    FutureFuture<?> ff = new FutureFuture<Object>();
    scheduler.schedule(new DelayedExecution(task, priority, ff), 
                       delayInMs, priority);
    
    return ff;
  }

  /**
   * Not currently implemented for limiter.
   * 
   * @throws UnsupportedOperationException exception always thrown
   */
  @Override
  public <T> Future<T> submitScheduled(Callable<T> task, long delayInMs,
                                       TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    }
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
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
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    }
    if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
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
    private final FutureFuture<?> future;

    public DelayedExecution(Runnable runnable, 
                            TaskPriority priority, 
                            FutureFuture<?> future) {
      this.runnable = runnable;
      this.priority = priority;
      this.future = future;
    }
    
    @Override
    public void run() {
      if (future == null) {
        execute(runnable, priority);
      } else {
        submit(runnable, priority, future);
      }
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
    private final FutureFuture<?> future;
    
    public PriorityRunnableWrapper(Runnable runnable, 
                                   TaskPriority priority, 
                                   FutureFuture<?> future) {
      this.runnable = runnable;
      this.priority = priority;
      this.future = future;
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
  
  /**
   * Future which contains a parent future 
   * (which may not be created yet).
   * 
   * @author jent - Mike Jensen
   * @param <T> result type returned by .get()
   */
  protected class FutureFuture<T> implements Future<T> {
    private boolean canceled;
    private boolean mayInterruptIfRunningOnCancel;
    private Future<?> parentFuture;
    
    private FutureFuture() {
      canceled = false;
      parentFuture = null;
    }
    
    private void setParentFuture(Future<?> parentFuture) {
      synchronized (this) {
        this.parentFuture = parentFuture;
        if (canceled) {
          parentFuture.cancel(mayInterruptIfRunningOnCancel);
        }
        
        this.notifyAll();
      }
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this) {
        canceled = true;
        mayInterruptIfRunningOnCancel = mayInterruptIfRunning;
        if (parentFuture != null) {
          return parentFuture.cancel(mayInterruptIfRunning);
        } else {
          return true;  // this is not guaranteed to be true, but is likely
        }
      }
    }

    @Override
    public boolean isCancelled() {
      synchronized (this) {
        return canceled;
      }
    }

    @Override
    public boolean isDone() {
      synchronized (this) {
        if (parentFuture == null) {
          return false;
        } else {
          return parentFuture.isDone();
        }
      }
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      try {
        return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // basically impossible
        throw ExceptionUtils.makeRuntime(e);
      }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      synchronized (this) {
      // TODO Auto-generated method stub
      return null;
      }
    }
  }
}
