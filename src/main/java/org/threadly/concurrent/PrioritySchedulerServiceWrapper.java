package org.threadly.concurrent;

import java.util.List;
import java.util.concurrent.Callable;

import org.threadly.concurrent.PriorityScheduler.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduler.QueueSet;
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
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableRunnableFuture<Void> taskFuture = new ListenableFutureTask<Void>(false, task);
    OneTimeTaskWrapper ottw;
    TaskPriority priority = pScheduler.getDefaultPriority();
    QueueSet queueSet = pScheduler.taskConsumer.getQueueSet(priority);
    if (delayInMillis == 0) {
      ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis, queueSet.executeQueue);
      pScheduler.addToExecuteQueue(queueSet, ottw);
    } else {
      ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis, queueSet.scheduleQueue);
      pScheduler.addToScheduleQueue(queueSet, ottw);
    }
    
    return new ScheduledFutureDelegate<Void>(taskFuture, new DelayedTaskWrapper(ottw));
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<V>(false, callable);
    OneTimeTaskWrapper ottw;
    TaskPriority priority = pScheduler.getDefaultPriority();
    QueueSet queueSet = pScheduler.taskConsumer.getQueueSet(priority);
    if (delayInMillis == 0) {
      ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis, queueSet.executeQueue);
      pScheduler.addToExecuteQueue(queueSet, ottw);
    } else {
      ottw = new OneTimeTaskWrapper(taskFuture, delayInMillis, queueSet.scheduleQueue);
      pScheduler.addToScheduleQueue(queueSet, ottw);
    }
    
    return new ScheduledFutureDelegate<V>(taskFuture, new DelayedTaskWrapper(ottw));
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelayInMs,
                                                                long delayInMs) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableRunnableFuture<Void> taskFuture = new ListenableFutureTask<Void>(true, task);
    TaskPriority priority = pScheduler.getDefaultPriority();
    QueueSet queueSet = pScheduler.taskConsumer.getQueueSet(priority);
    RecurringDelayTaskWrapper rdtw = new RecurringDelayTaskWrapper(taskFuture, queueSet,
                                                                   initialDelayInMs, delayInMs);
    pScheduler.addToScheduleQueue(queueSet, rdtw);
    
    return new ScheduledFutureDelegate<Void>(taskFuture, new DelayedTaskWrapper(rdtw));
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelayInMillis,
                                                             long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(pScheduler, task);
    
    ListenableRunnableFuture<Void> taskFuture = new ListenableFutureTask<Void>(true, task);
    TaskPriority priority = pScheduler.getDefaultPriority();
    QueueSet queueSet = pScheduler.taskConsumer.getQueueSet(priority);
    RecurringRateTaskWrapper rrtw = new RecurringRateTaskWrapper(taskFuture, queueSet,
                                                                 initialDelayInMillis, periodInMillis);
    pScheduler.addToScheduleQueue(queueSet, rrtw);
    
    return new ScheduledFutureDelegate<Void>(taskFuture, new DelayedTaskWrapper(rrtw));
  }
}
