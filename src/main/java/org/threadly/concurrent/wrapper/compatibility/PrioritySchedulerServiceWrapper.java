package org.threadly.concurrent.wrapper.compatibility;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.InternalAccessor;
import org.threadly.concurrent.PriorityScheduler;
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
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return pScheduler.awaitTermination(unit.toMillis(timeout));
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableRunnableFuture<Void> taskFuture = new ListenableFutureTask<Void>(false, task);
    Delayed d = PrioritySchedulerAccessor.doScheduleAndGetDelayed(pScheduler, taskFuture, delayInMillis);
    
    return new ScheduledFutureDelegate<Void>(taskFuture, d);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableRunnableFuture<V> taskFuture = new ListenableFutureTask<V>(false, callable);
    Delayed d = PrioritySchedulerAccessor.doScheduleAndGetDelayed(pScheduler, taskFuture, delayInMillis);
    
    return new ScheduledFutureDelegate<V>(taskFuture, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelay, long delayInMs) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableRunnableFuture<Void> lft = new CancelRemovingListenableFutureTask<Void>(scheduler, 
                                                                                      true, task);
    Delayed d = PrioritySchedulerAccessor.doScheduleWithFixedDelayAndGetDelayed(pScheduler, lft, 
                                                                                initialDelay, delayInMs);
    
    return new ScheduledFutureDelegate<Void>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelay, long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(pScheduler, task);
    
    ListenableRunnableFuture<Void> lft = new CancelRemovingListenableFutureTask<Void>(scheduler, 
                                                                                      true, task);
    Delayed d = PrioritySchedulerAccessor.doScheduleAtFixedRateAndGetDelayed(pScheduler, lft, 
                                                                             initialDelay, periodInMillis);
    
    return new ScheduledFutureDelegate<Void>(lft, d);
  }
  
  /**
   * <p>Just used to gain visibility through {@link InternalAccessor}.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.6.0
   */
  private static class PrioritySchedulerAccessor extends InternalAccessor {
    public static Delayed doScheduleAndGetDelayed(PriorityScheduler pScheduler, Runnable task, 
                                                  long delayInMillis) {
      return InternalAccessor.doScheduleAndGetDelayed(pScheduler, task, delayInMillis);
    }
    
    public static Delayed doScheduleAtFixedRateAndGetDelayed(PriorityScheduler pScheduler, 
                                                             Runnable task, 
                                                             long initialDelay, long periodInMillis) {
      return InternalAccessor.doScheduleAtFixedRateAndGetDelayed(pScheduler, task, 
                                                                 initialDelay, periodInMillis);
    }
    
    public static Delayed doScheduleWithFixedDelayAndGetDelayed(PriorityScheduler pScheduler, 
                                                                Runnable task, 
                                                                long initialDelay, long delayInMs) {
      return InternalAccessor.doScheduleWithFixedDelayAndGetDelayed(pScheduler, task, 
                                                                    initialDelay, delayInMs);
    }
  }
}
