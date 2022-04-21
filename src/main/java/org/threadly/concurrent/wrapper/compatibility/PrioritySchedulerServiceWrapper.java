package org.threadly.concurrent.wrapper.compatibility;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.ThreadlyInternalAccessor;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableRunnableFuture;
import org.threadly.concurrent.wrapper.priority.DefaultPriorityWrapper;

/**
 * This is a wrapper for {@link PriorityScheduler} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (AKA the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those are 
 * pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).
 * 
 * @since 4.6.0 (since 1.0.0 as org.threadly.concurrent.PriorityScheduledExecutorServiceWrapper)
 */
public class PrioritySchedulerServiceWrapper extends AbstractExecutorServiceWrapper {
  protected final PriorityScheduler pScheduler;
  protected final TaskPriority taskPriority;
  
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler {@link PriorityScheduler} implementation to rely on
   */
  public PrioritySchedulerServiceWrapper(PriorityScheduler scheduler) {
    this(scheduler, null);
  }
  
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler {@link PriorityScheduler} implementation to rely on
   * @param taskPriority Priority for all tasks submitted to the parent scheduler
   */
  public PrioritySchedulerServiceWrapper(PriorityScheduler scheduler, TaskPriority taskPriority) {
    super(DefaultPriorityWrapper.ensurePriority(scheduler, taskPriority));
    
    this.pScheduler = scheduler;
    this.taskPriority = taskPriority == null ? scheduler.getDefaultPriority() : taskPriority;
  }

  @Override
  public void shutdown() {
    pScheduler.shutdown();
  }

  /**
   * This call will stop the processor as quick as possible.  Any tasks which are awaiting 
   * execution will be canceled and returned as a result to this call.
   * <p>
   * Unlike {@link java.util.concurrent.ExecutorService} implementation there is no attempt to 
   * stop any currently execution tasks.
   * <p>
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
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return pScheduler.awaitTermination(unit.toMillis(timeout));
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableRunnableFuture<Void> taskFuture = new ListenableFutureTask<>(task, null, pScheduler);
    Delayed d = ThreadlyInternalAccessor.doScheduleAndGetDelayed(pScheduler, taskFuture, 
                                                                 taskPriority, delayInMillis);
    
    return new ScheduledFutureDelegate<>(taskFuture, d);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<>(callable, pScheduler);
    Delayed d = ThreadlyInternalAccessor.doScheduleAndGetDelayed(pScheduler, taskFuture, 
                                                                 taskPriority, delayInMillis);
    
    return new ScheduledFutureDelegate<>(taskFuture, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelay, long delayInMs) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableRunnableFuture<Void> lft = 
        new CancelRemovingListenableFutureTask<>(scheduler, true, task, pScheduler);
    Delayed d = ThreadlyInternalAccessor.doScheduleWithFixedDelayAndGetDelayed(pScheduler, 
                                                                               lft, taskPriority, 
                                                                               initialDelay, 
                                                                               delayInMs);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelay, long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(pScheduler, task);
    
    ListenableRunnableFuture<Void> lft = 
        new CancelRemovingListenableFutureTask<>(scheduler, true, task, pScheduler);
    Delayed d = ThreadlyInternalAccessor.doScheduleAtFixedRateAndGetDelayed(pScheduler, 
                                                                            lft, taskPriority, 
                                                                            initialDelay, 
                                                                            periodInMillis);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }
}
