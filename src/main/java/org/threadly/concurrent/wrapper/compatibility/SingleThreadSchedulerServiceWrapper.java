package org.threadly.concurrent.wrapper.compatibility;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.ThreadlyInternalAccessor;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;
import org.threadly.concurrent.wrapper.priority.DefaultPriorityWrapper;

/**
 * This is a wrapper for {@link SingleThreadScheduler} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (AKA the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those are 
 * pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).
 * 
 * @since 4.6.0 (since 2.0.0 at org.threadly.concurrent)
 */
public class SingleThreadSchedulerServiceWrapper extends AbstractExecutorServiceWrapper {
  protected final SingleThreadScheduler singleThreadScheduler;
  protected final TaskPriority taskPriority;

  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public SingleThreadSchedulerServiceWrapper(SingleThreadScheduler scheduler) {
    this(scheduler, null);
  }
  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   * @param taskPriority Priority for all tasks submitted to the parent scheduler
   */
  public SingleThreadSchedulerServiceWrapper(SingleThreadScheduler scheduler, 
                                             TaskPriority taskPriority) {
    super(DefaultPriorityWrapper.ensurePriority(scheduler, taskPriority));
    
    this.singleThreadScheduler = scheduler;
    this.taskPriority = taskPriority == null ? scheduler.getDefaultPriority() : taskPriority;
  }

  @Override
  public void shutdown() {
    singleThreadScheduler.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return singleThreadScheduler.shutdownNow();
  }

  @Override
  public boolean isTerminated() {
    return singleThreadScheduler.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return singleThreadScheduler.awaitTermination(unit.toMillis(timeout));
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableFutureTask<Void> lft = new ListenableFutureTask<>(false, task);
    Delayed d = ThreadlyInternalAccessor.doScheduleAndGetDelayed(singleThreadScheduler, 
                                                                 lft, taskPriority, delayInMillis);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableFutureTask<V> lft = new ListenableFutureTask<>(false, callable);
    Delayed d = ThreadlyInternalAccessor.doScheduleAndGetDelayed(singleThreadScheduler, 
                                                                 lft, taskPriority, delayInMillis);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelay, long delayInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new CancelRemovingListenableFutureTask<>(scheduler, true, task);
    Delayed d = ThreadlyInternalAccessor.doScheduleWithFixedDelayAndGetDelayed(singleThreadScheduler, 
                                                                               lft, taskPriority, 
                                                                               initialDelay, 
                                                                               delayInMillis);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelay, long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new CancelRemovingListenableFutureTask<>(scheduler, true, task);
    Delayed d = ThreadlyInternalAccessor.doScheduleAtFixedRateAndGetDelayed(singleThreadScheduler, 
                                                                            lft, taskPriority, 
                                                                            initialDelay, 
                                                                            periodInMillis);
    
    return new ScheduledFutureDelegate<>(lft, d);
  }
}
