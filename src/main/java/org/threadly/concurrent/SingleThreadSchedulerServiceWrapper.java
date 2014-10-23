package org.threadly.concurrent;

import java.util.List;
import java.util.concurrent.Callable;

import org.threadly.concurrent.SingleThreadScheduler.SchedulerManager;
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
  private final SingleThreadScheduler scheduler;

  /**
   * Constructs a new wrapper to adhere to the 
   * {@link java.util.concurrent.ScheduledExecutorService} interface.
   * 
   * @param scheduler scheduler implementation to rely on
   */
  public SingleThreadSchedulerServiceWrapper(SingleThreadScheduler scheduler) {
    super(scheduler);
    
    this.scheduler = scheduler;
  }

  @Override
  public void shutdown() {
    scheduler.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return scheduler.shutdownNow();
  }

  @Override
  public boolean isTerminated() {
    SchedulerManager sm = scheduler.sManager.get();
    if (sm == null || sm.isRunning()) {
      return false;
    } else {
      return ! sm.execThread.isAlive();
    }
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableFutureTask<Object> lft = new ListenableFutureTask<Object>(false, task);
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    NoThreadScheduler.OneTimeTask ott = nts.new OneTimeTask(lft, delayInMillis);
    nts.add(ott);
    
    return new ScheduledFutureDelegate<Object>(lft, ott);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableFutureTask<V> lft = new ListenableFutureTask<V>(false, callable);
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    NoThreadScheduler.OneTimeTask ott = nts.new OneTimeTask(lft, delayInMillis);
    nts.add(ott);
    
    return new ScheduledFutureDelegate<V>(lft, ott);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelayInMillis,
                                                                long delayInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Object> lft = new ListenableFutureTask<Object>(true, task);
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    NoThreadScheduler.RecurringTask rt = nts.new RecurringDelayTask(lft, initialDelayInMillis, delayInMillis);
    nts.add(rt);
    
    return new ScheduledFutureDelegate<Object>(lft, rt);
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelayInMillis,
                                                             long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Object> lft = new ListenableFutureTask<Object>(true, task);
    NoThreadScheduler nts = scheduler.getRunningScheduler();
    NoThreadScheduler.RecurringTask rt = nts.new RecurringRateTask(lft, initialDelayInMillis, periodInMillis);
    nts.add(rt);
    
    return new ScheduledFutureDelegate<Object>(lft, rt);
  }
}
