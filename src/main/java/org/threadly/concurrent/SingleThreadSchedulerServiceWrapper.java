package org.threadly.concurrent;

import java.util.List;
import java.util.concurrent.Callable;

import org.threadly.concurrent.AbstractPriorityScheduler.QueueSet;
import org.threadly.concurrent.NoThreadScheduler.NoThreadRecurringDelayTaskWrapper;
import org.threadly.concurrent.NoThreadScheduler.NoThreadRecurringRateTaskWrapper;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;
import org.threadly.util.Clock;

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
    if (! singleThreadScheduler.sManager.hasBeenStopped()) {
      return false;
    } else {
      return ! singleThreadScheduler.sManager.execThread.isAlive();
    }
  }

  @Override
  protected ListenableScheduledFuture<?> schedule(Runnable task, long delayInMillis) {
    ListenableFutureTask<Void> lft = new ListenableFutureTask<Void>(false, task);
    NoThreadScheduler nts = singleThreadScheduler.getRunningScheduler();
    NoThreadScheduler.OneTimeTaskWrapper ott = nts.doSchedule(lft, delayInMillis, 
                                                              nts.getDefaultPriority());
    
    return new ScheduledFutureDelegate<Void>(lft, new DelayedTaskWrapper(ott));
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableFutureTask<V> lft = new ListenableFutureTask<V>(false, callable);
    NoThreadScheduler nts = singleThreadScheduler.getRunningScheduler();
    NoThreadScheduler.OneTimeTaskWrapper ott = nts.doSchedule(lft, delayInMillis, 
                                                              nts.getDefaultPriority());
    
    return new ScheduledFutureDelegate<V>(lft, new DelayedTaskWrapper(ott));
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleWithFixedDelay(Runnable task,
                                                                long initialDelay,
                                                                long delayInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new ListenableFutureTask<Void>(true, task);
    NoThreadScheduler nts = singleThreadScheduler.getRunningScheduler();
    QueueSet queueSet = nts.getQueueSet(nts.getDefaultPriority());
    NoThreadRecurringDelayTaskWrapper rdt = 
        nts.new NoThreadRecurringDelayTaskWrapper(lft, queueSet, 
                                                  Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                  delayInMillis);
    queueSet.addScheduled(rdt);
    
    return new ScheduledFutureDelegate<Void>(lft, new DelayedTaskWrapper(rdt));
  }

  @Override
  protected ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                             long initialDelay,
                                                             long periodInMillis) {
    // wrap the task to ensure the correct behavior on exceptions
    task = new ThrowableHandlingRecurringRunnable(scheduler, task);
    
    ListenableFutureTask<Void> lft = new ListenableFutureTask<Void>(true, task);
    NoThreadScheduler nts = singleThreadScheduler.getRunningScheduler();
    QueueSet queueSet = nts.getQueueSet(nts.getDefaultPriority());
    NoThreadRecurringRateTaskWrapper rt = 
        nts.new NoThreadRecurringRateTaskWrapper(lft, queueSet, 
                                                  Clock.accurateForwardProgressingMillis() + initialDelay, 
                                                  periodInMillis);
    queueSet.addScheduled(rt);
    
    return new ScheduledFutureDelegate<Void>(lft, new DelayedTaskWrapper(rt));
  }
}
