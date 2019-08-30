package org.threadly.concurrent.future.watchdog;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;

/**
 * Abstract implementation for Watchdog.  Providing a global thread pool that watchdogs can default 
 * to, as well as some minor code duplication reductions.
 * 
 * @since 5.40
 */
class AbstractWatchdog {
  private static final AtomicReference<SubmitterScheduler> STATIC_SCHEDULER = 
      new AtomicReference<>();
  
  protected static final SubmitterScheduler getStaticScheduler() {
    SubmitterScheduler ss = STATIC_SCHEDULER.get();
    if (ss == null) {
      STATIC_SCHEDULER.compareAndSet(null, CentralThreadlyPool.threadPool(2, "WatchdogDefaultScheduler"));
      ss = STATIC_SCHEDULER.get();
    }
    
    return ss;
  }

  protected final Collection<Object> futures;
  protected final ReschedulingOperation checkRunner;
  
  protected AbstractWatchdog(Function<Collection<?>, ReschedulingOperation> checkFactory) {
    this.futures = new ConcurrentLinkedQueue<>();
    this.checkRunner = checkFactory.apply(futures);
  }
  
  /**
   * Checks to see if this watchdog is currently active.  Meaning there are futures on it which 
   * either have not been completed yet, or have not been inspected for completion.  If this 
   * returns false, it means that there are no futures waiting to complete, and no scheduled tasks 
   * currently scheduled to inspect them.
   * 
   * @return {@code true} if this watchdog is currently in use
   */
  public boolean isActive() {
    return checkRunner.isActive() || ! futures.isEmpty();
  }
  
  /**
   * Check how many futures are currently being monitored for completion.
   * 
   * @return The number of futures being monitored
   */
  public int getWatchingCount() {
    return futures.size();
  }
  
  protected void watchWrapper(Object fw, ListenableFuture<?> future) {
    futures.add(fw);
    // we attempt to remove the future on completion to reduce inspection needed
    future.listener(new WrapperRemover(fw), SameThreadSubmitterExecutor.instance());
    
    checkRunner.signalToRun();
  }
  
  /**
   * Listener implementation for removing the wrapper from the queue when it completes (and thus 
   * invokes this).
   * 
   * @since 5.40
   */
  protected class WrapperRemover implements Runnable {
    private final Object fw;
    
    protected WrapperRemover(Object fw) {
      this.fw = fw;
    }
    
    @Override
    public void run() {
      futures.remove(fw);
    }
  }
}
