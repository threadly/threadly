package org.threadly.concurrent.wrapper.compatibility;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.InternalAccessor;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;

/**
 * <p>This is a wrapper for {@link SingleThreadScheduler} to be a drop in replacement for any 
 * {@link java.util.concurrent.ScheduledExecutorService} (AKA the 
 * {@link java.util.concurrent.ScheduledThreadPoolExecutor} 
 * interface). It does make some performance sacrifices to adhere to this interface, but those are 
 * pretty minimal.  The largest compromise in here is easily scheduleAtFixedRate (which you should 
 * read the javadocs for if you need).</p>
 * 
 * @author jent - Mike Jensen
 * @since 2.0.0
 */
public class SingleThreadSchedulerServiceWrapper extends AbstractExecutorServiceWrapper {
  protected final SingleThreadScheduler singleThreadScheduler;

  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public SingleThreadSchedulerServiceWrapper(SingleThreadScheduler scheduler) {
    super(scheduler);
    
    this.singleThreadScheduler = scheduler;
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
    ListenableFutureTask<Void> lft = new ListenableFutureTask<Void>(false, task);
    Delayed d = SingleThreadAccessor.doScheduleAndGetDelayed(singleThreadScheduler, 
                                                             lft, delayInMillis);
    
    return new ScheduledFutureDelegate<Void>(lft, d);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableFutureTask<V> lft = new ListenableFutureTask<V>(false, callable);
    Delayed d = SingleThreadAccessor.doScheduleAndGetDelayed(singleThreadScheduler, 
                                                             lft, delayInMillis);
    
    return new ScheduledFutureDelegate<V>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelay, long delayInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new CancelRemovingListenableFutureTask<Void>(scheduler, 
                                                                                  true, task);
    Delayed d = SingleThreadAccessor.doScheduleWithFixedDelayAndGetDelayed(singleThreadScheduler, lft, 
                                                                           initialDelay, delayInMillis);
    
    return new ScheduledFutureDelegate<Void>(lft, d);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelay, long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new CancelRemovingListenableFutureTask<Void>(scheduler, 
                                                                                  true, task);
    Delayed d = SingleThreadAccessor.doScheduleAtFixedRateAndGetDelayed(singleThreadScheduler, lft, 
                                                                        initialDelay, periodInMillis);
    
    return new ScheduledFutureDelegate<Void>(lft, d);
  }
  
  /**
   * <p>Just used to gain visibility through {@link InternalAccessor}.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.6.0
   */
  private static class SingleThreadAccessor extends InternalAccessor {
    public static Delayed doScheduleAndGetDelayed(SingleThreadScheduler scheduler, Runnable task, 
                                                  long delayInMillis) {
      return InternalAccessor.doScheduleAndGetDelayed(scheduler, task, delayInMillis);
    }
    
    public static Delayed doScheduleAtFixedRateAndGetDelayed(SingleThreadScheduler scheduler, 
                                                             Runnable task, 
                                                             long initialDelay, long periodInMillis) {
      return InternalAccessor.doScheduleAtFixedRateAndGetDelayed(scheduler, task, 
                                                                 initialDelay, periodInMillis);
    }
    
    public static Delayed doScheduleWithFixedDelayAndGetDelayed(SingleThreadScheduler scheduler, 
                                                                Runnable task, 
                                                                long initialDelay, long delayInMs) {
      return InternalAccessor.doScheduleWithFixedDelayAndGetDelayed(scheduler, task, 
                                                                    initialDelay, delayInMs);
    }
  }
}
