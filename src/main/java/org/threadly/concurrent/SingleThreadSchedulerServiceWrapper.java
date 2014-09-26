package org.threadly.concurrent;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.threadly.concurrent.NoThreadScheduler.TaskContainer;
import org.threadly.concurrent.SingleThreadScheduler.SchedulerManager;
import org.threadly.concurrent.future.ListenableFutureTask;
import org.threadly.concurrent.future.ListenableScheduledFuture;
import org.threadly.concurrent.future.ScheduledFutureDelegate;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ListUtils;

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

  /**
   * This call is no different from the shutdown() call.  Currently the 
   * {@link SingleThreadScheduler} implementation has no way to stop executing tasks it has 
   * already taken on it's current "tick" cycle.
   *
   * @return Empty list
   */
  @Override
  public List<Runnable> shutdownNow() {
    /* we currently don't have an easy wait to stop a 
     * .tick() call in progress on the scheduler thread.
     */
    scheduler.shutdown();
    
    return Collections.emptyList();
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
    NoThreadScheduler nts = scheduler.getScheduler();
    NoThreadScheduler.OneTimeTask ott = nts.new OneTimeTask(lft, delayInMillis);
    nts.add(ott);
    
    return new ScheduledFutureDelegate<Object>(lft, ott);
  }

  @Override
  protected <V> ListenableScheduledFuture<V> schedule(Callable<V> callable, long delayInMillis) {
    ListenableFutureTask<V> lft = new ListenableFutureTask<V>(false, callable);
    NoThreadScheduler nts = scheduler.getScheduler();
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
    NoThreadScheduler nts = scheduler.getScheduler();
    NoThreadScheduler.RecurringTask ott = nts.new RecurringTask(lft, initialDelayInMillis, delayInMillis);
    nts.add(ott);
    
    return new ScheduledFutureDelegate<Object>(lft, ott);
  }

  @Override
  public ListenableScheduledFuture<?> scheduleAtFixedRate(Runnable task,
                                                          long initialDelay, long period,
                                                          TimeUnit unit) {
    if (task == null) {
      throw new NullPointerException("Must provide task");
    }
    ArgumentVerifier.assertGreaterThanZero(period, "period");
    if (initialDelay < 0) {
      initialDelay = 0;
    }
    
    ListenableFutureTask<Object> lft = new ListenableFutureTask<Object>(true, task);
    NoThreadScheduler nts = scheduler.getScheduler();
    FixedRateRecurringTask frrt = new FixedRateRecurringTask(nts, lft, 
                                                             unit.toMillis(initialDelay), 
                                                             unit.toMillis(period));
    nts.add(frrt);
    
    return new ScheduledFutureDelegate<Object>(lft, frrt);
  }

  /**
   * <p>Container for runnables which are to run at a fixed rate.</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.1.0
   */
  protected class FixedRateRecurringTask extends TaskContainer {
    private final NoThreadScheduler scheduler;
    private final long initialDelay;
    private final long period;
    private long nextRunTime;
    
    public FixedRateRecurringTask(NoThreadScheduler scheduler, 
                                  Runnable runnable, long initialDelay, long period) {
      super(runnable);
      
      this.scheduler = scheduler;
      this.initialDelay = initialDelay;
      this.period = period;
      nextRunTime = -1;
    }
    
    @Override
    protected void setInitialDelay() {
      nextRunTime = scheduler.nowInMillis() + initialDelay;
    }
    
    @Override
    public void prepareForRun() {
      // task will only be rescheduled once complete
    }
    
    @Override
    public void runComplete() {
      synchronized (scheduler.taskQueue.getModificationLock()) {
        scheduler.startInsertion();
        try {
          // almost certainly will be the first item in the queue
          int currentIndex = scheduler.taskQueue.indexOf(this);
          if (currentIndex < 0) {
            // task was removed from queue, do not re-insert
            return;
          }
          nextRunTime += period;
          long nextDelay = nextRunTime - scheduler.nowInMillis();
          if (nextDelay < 0) {
            nextDelay = 0;
          }
          int insertionIndex = ListUtils.getInsertionEndIndex(scheduler.taskQueue, nextDelay, true);
          
          scheduler.taskQueue.reposition(currentIndex, insertionIndex);
          
        } finally {
          scheduler.endInsertion();
        }
      }
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
      return timeUnit.convert(nextRunTime - scheduler.nowInMillis(), 
                              TimeUnit.MILLISECONDS);
    }
  }
}
