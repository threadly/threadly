package org.threadly.concurrent;

import java.util.List;
import java.util.concurrent.Callable;

import org.threadly.concurrent.PriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.RecurringDelayTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.RecurringRateTaskWrapper;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;

/**
 * <p>This is a wrapper for {@link PriorityScheduler} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (AKA the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those are 
 * pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.2.0 (existed since 1.0.0 as PriorityScheduledExecutorServiceWrapper)
 */
public class PrioritySchedulerServiceWrapper extends AbstractExecutorServiceWrapper {
  protected final PriorityScheduler pScheduler;
  
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler {@link PriorityScheduler} implementation to rely on
   */
  public PrioritySchedulerServiceWrapper(PriorityScheduler scheduler) {
    super(scheduler);
    
    this.pScheduler = scheduler;
  }

  @Override
  public void shutdown() {
    pScheduler.shutdown();
  }

  /**
   * This call will stop the processor as quick as possible.  Any tasks which are awaiting 
   * execution will be canceled and returned as a result to this call.
   * 
   * Unlike {@link java.util.concurrent.ExecutorService} implementation there is no attempt to 
   * stop any currently execution tasks.
   *
   * This method does not wait for actively executing tasks toterminate.  Use 
   * {@link #awaitTermination awaitTermination} to do that.
   *
   * @return list of tasks that never commenced execution
   */
  @Override
  public List<Runnable> shutdownNow() {
    return pScheduler.shutdownNow();
  }

  @Override
  public boolean isTerminated() {
    return scheduler.isShutdown() && 
           pScheduler.getCurrentPoolSize() == 0;
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable command, long delayInMillis) {
    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(false, command);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis);
    if (delayInMillis == 0) {
      pScheduler.addToExecuteQueue(pScheduler.getDefaultPriority(), ottw);
    } else {
      pScheduler.addToScheduleQueue(pScheduler.getDefaultPriority(), ottw);
    }
    
    return new ScheduledFutureDelegate<Object>(taskFuture, ottw);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<V>(false, callable);
    OneTimeTaskWrapper ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis);
    if (delayInMillis == 0) {
      pScheduler.addToExecuteQueue(pScheduler.getDefaultPriority(), ottw);
    } else {
      pScheduler.addToScheduleQueue(pScheduler.getDefaultPriority(), ottw);
    }
    
    return new ScheduledFutureDelegate<V>(taskFuture, ottw);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelayInMs,
                                                                long delayInMs) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(true, task);
    TaskPriority priority = pScheduler.getDefaultPriority();
    RecurringDelayTaskWrapper rdtw = new RecurringDelayTaskWrapper(taskFuture, 
                                                                   pScheduler.getQueueManager(priority),
                                                                   initialDelayInMs, delayInMs);
    pScheduler.addToScheduleQueue(priority, rdtw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, rdtw);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelayInMillis,
                                                             long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(pScheduler, task);
    
    ListenableRunnableFuture<Object> taskFuture = new ListenableFutureTask<Object>(true, task);
    TaskPriority priority = pScheduler.getDefaultPriority();
    RecurringRateTaskWrapper rrtw = new RecurringRateTaskWrapper(taskFuture, 
                                                                 pScheduler.getQueueManager(priority),
                                                                 initialDelayInMillis, periodInMillis);
    pScheduler.addToScheduleQueue(priority, rrtw);
    
    return new ScheduledFutureDelegate<Object>(taskFuture, rrtw);
  }
}
