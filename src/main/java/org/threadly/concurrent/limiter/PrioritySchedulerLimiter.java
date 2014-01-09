package org.threadly.concurrent.limiter;

import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.threadly.concurrent.PrioritySchedulerInterface;
import org.threadly.concurrent.RunnableContainerInterface;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.future.ListenableFutureTask;

/**
 * <p>This class is designed to limit how much parallel execution happens 
 * on a provided {@link PrioritySchedulerInterface}.  This allows the 
 * implementor to have one thread pool for all their code, and if 
 * they want certain sections to have less levels of parallelism 
 * (possibly because those those sections would completely consume the 
 * global pool), they can wrap the executor in this class.</p>
 * 
 * <p>Thus providing you better control on the absolute thread count and 
 * how much parallelism can occur in different sections of the program.</p>
 * 
 * <p>This is an alternative from having to create multiple thread pools.  
 * By using this you also are able to accomplish more efficiently thread use 
 * than multiple thread pools would.</p>
 * 
 * @author jent - Mike Jensen
 */
public class PrioritySchedulerLimiter extends AbstractThreadPoolLimiter 
                                      implements PrioritySchedulerInterface {
  protected final PrioritySchedulerInterface scheduler;
  protected final Queue<PriorityWrapper> waitingTasks;
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerInterface}.
   * 
   * @param scheduler {@link PrioritySchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   */
  public PrioritySchedulerLimiter(PrioritySchedulerInterface scheduler, 
                                  int maxConcurrency) {
    this(scheduler, maxConcurrency, null);
  }
  
  /**
   * Constructs a new limiter that implements the {@link PrioritySchedulerInterface}.
   * 
   * @param scheduler {@link PrioritySchedulerInterface} implementation to submit task executions to.
   * @param maxConcurrency maximum qty of runnables to run in parallel
   * @param subPoolName name to describe threads while tasks running in pool (null to not change thread names)
   */
  public PrioritySchedulerLimiter(PrioritySchedulerInterface scheduler, 
                                  int maxConcurrency, String subPoolName) {
    super(maxConcurrency, subPoolName);
    
    if (scheduler == null) {
      throw new IllegalArgumentException("Must provide scheduler");
    }
    
    this.scheduler = scheduler;
    waitingTasks = new ConcurrentLinkedQueue<PriorityWrapper>();
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
        PriorityWrapper pw = waitingTasks.poll();
        scheduler.execute(pw, pw.getPriority());
      }
    }
  }

  @Override
  public void execute(Runnable task) {
    execute(task, null);
  }

  @Override
  public void execute(Runnable task, TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide runnable");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    PriorityWrapper pw = new PriorityWrapper(task, priority);
    if (canRunTask()) {  // try to avoid adding to queue if we can
      scheduler.execute(pw, priority);
    } else {
      waitingTasks.add(pw);
      consumeAvailable(); // call to consume in case task finished after first check
    }
  }

  @Override
  public ListenableFuture<?> submit(Runnable task) {
    return submit(task, null);
  }

  @Override
  public ListenableFuture<?> submit(Runnable task, TaskPriority priority) {
    return submit(task, null, priority);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result) {
    return submit(task, result, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result, 
                                        TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task, result);
    
    execute(lft, priority);
    
    return lft;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task) {
    return submit(task, null);
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task, 
                                        TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> lft = new ListenableFutureTask<T>(false, task);
    
    execute(lft, priority);
    
    return lft;
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs) {
    return submitScheduled(task, delayInMs, null);
  }

  @Override
  public ListenableFuture<?> submitScheduled(Runnable task, long delayInMs, 
                                             TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<?> ft = new ListenableFutureTask<Object>(false, task);
    
    schedule(ft, delayInMs, priority);
    
    return ft;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs) {
    return submitScheduled(task, result, delayInMs, null);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Runnable task, T result, long delayInMs, 
                                                 TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<T>(false, task, result);
    
    schedule(ft, delayInMs, priority);
    
    return ft;
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs) {
    return submitScheduled(task, delayInMs, null);
  }

  @Override
  public <T> ListenableFuture<T> submitScheduled(Callable<T> task, long delayInMs, 
                                                 TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide task");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    ListenableFutureTask<T> ft = new ListenableFutureTask<T>(false, task);
    
    schedule(ft, delayInMs, priority);
    
    return ft;
  }

  @Override
  public void schedule(Runnable task, long delayInMs) {
    schedule(task, delayInMs, null);
  }

  @Override
  public void schedule(Runnable task, long delayInMs, 
                       TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (delayInMs < 0) {
      throw new IllegalArgumentException("delayInMs must be >= 0");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    if (delayInMs == 0) {
      execute(task, priority);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(task, priority), 
                         delayInMs, TaskPriority.High);
    }
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay) {
    scheduleWithFixedDelay(task, initialDelay, recurringDelay, 
                           null);
  }

  @Override
  public void scheduleWithFixedDelay(Runnable task, long initialDelay,
                                     long recurringDelay, TaskPriority priority) {
    if (task == null) {
      throw new IllegalArgumentException("Must provide a task");
    } else if (initialDelay < 0) {
      throw new IllegalArgumentException("initialDelay must be >= 0");
    } else if (recurringDelay < 0) {
      throw new IllegalArgumentException("recurringDelay must be >= 0");
    } else if (priority == null) {
      priority = scheduler.getDefaultPriority();
    }
    
    RecurringRunnableWrapper rrw = new RecurringRunnableWrapper(task, recurringDelay, priority);
    
    if (initialDelay == 0) {
      execute(rrw);
    } else {
      scheduler.schedule(new DelayedExecutionRunnable<Object>(rrw, priority), 
                         initialDelay, TaskPriority.High);
    }
  }

  @Override
  public boolean isShutdown() {
    return scheduler.isShutdown();
  }

  @Override
  public TaskPriority getDefaultPriority() {
    return scheduler.getDefaultPriority();
  }
  
  /**
   * <p>Small runnable that allows scheduled tasks to pass through 
   * the same execution queue that immediate execution has to.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class DelayedExecutionRunnable<T> implements Runnable, 
                                                         RunnableContainerInterface {
    private final Runnable runnable;
    private final TaskPriority priority;

    public DelayedExecutionRunnable(Runnable runnable, TaskPriority priority) {
      this.runnable = runnable;
      this.priority = priority;
    }
    
    @Override
    public void run() {
      execute(runnable, priority);
    }

    @Override
    public Runnable getContainedRunnable() {
      return runnable;
    }
  }

  /**
   * <p>Wrapper for tasks which are executed in this sub pool, 
   * this ensures that handleTaskFinished() will be called 
   * after the task completes.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class RecurringRunnableWrapper extends LimiterRunnableWrapper {
    private final long recurringDelay;
    private final DelayedExecutionRunnable<?> delayRunnable;
    
    public RecurringRunnableWrapper(Runnable runnable, 
                                    long recurringDelay, 
                                    TaskPriority priority) {
      super(runnable);
      
      this.recurringDelay = recurringDelay;
      delayRunnable = new DelayedExecutionRunnable<Object>(this, priority);
    }
    
    @Override
    protected void doAfterRunTasks() {
      scheduler.schedule(delayRunnable, recurringDelay, TaskPriority.High);
    }
  }

  /**
   * <p>Class that adds {@link TaskPriority} functionality to the 
   * {@link LimiterRunnableWrapper}.</p>
   * 
   * @author jent - Mike Jensen
   */
  protected class PriorityWrapper extends LimiterRunnableWrapper {
    private final TaskPriority priority;
    
    public PriorityWrapper(Runnable runnable, TaskPriority priority) {
      super(runnable);
      
      this.priority = priority;
    }
    
    public TaskPriority getPriority() {
      return priority;
    }
  }
}
