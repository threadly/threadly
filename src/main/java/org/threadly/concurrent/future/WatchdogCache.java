package org.threadly.concurrent.future;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.threadly.concurrent.SubmitterScheduler;

/**
 * <p>A class which handles a collection of  {@link Watchdog} instances.  Because the timeout for 
 * {@link Watchdog} is set in the constructor {@link Watchdog#Watchdog(long, boolean)}, you can 
 * use this class to be more flexible and set the timeout at the time of watching the future.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.0.0
 */
public class WatchdogCache {
  protected static final int INSPECTION_INTERVAL_MILLIS = 1000 * 10;
  
  protected final SubmitterScheduler scheduler;
  protected final boolean sendInterruptOnFutureCancel;
  protected final ConcurrentMap<Long, Watchdog> cachedDogs;
  protected final Function<Long, Watchdog> watchdogProducer;
  protected final Runnable cacheCleaner;
  private final AtomicBoolean cleanerScheduled;
  
  /**
   * Constructs a new {@link WatchdogCache}.  This constructor will use a default static scheduler 
   * (which is lazily constructed).  This should be fine in most cases, but you can provide your 
   * own scheduler if you want to avoid the thread creation (which is shared among all instances 
   * that were constructed with this constructor or {@link Watchdog#Watchdog(long, boolean)}}.
   * 
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public WatchdogCache(boolean sendInterruptOnFutureCancel) {
    this(Watchdog.getStaticScheduler(), sendInterruptOnFutureCancel);
  }

  /**
   * Constructs a new {@link WatchdogCache} with a scheduler of your choosing.  It is critical 
   * that this scheduler has a free thread available to inspect futures which may not have 
   * completed in the given timeout.  You may want to use a org.threadly.concurrent.limiter to 
   * ensure that there are threads available.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public WatchdogCache(SubmitterScheduler scheduler, boolean sendInterruptOnFutureCancel) {
    this.scheduler = scheduler;
    this.sendInterruptOnFutureCancel = sendInterruptOnFutureCancel;
    cachedDogs = new ConcurrentHashMap<Long, Watchdog>();
    watchdogProducer = (timeout) -> {
      return new Watchdog(scheduler, timeout, sendInterruptOnFutureCancel);
    };
    cacheCleaner = new CleanRunner();
    cleanerScheduled = new AtomicBoolean(false);
  }
  
  /**
   * Watch a given {@link ListenableFuture} to ensure that it completes within the provided 
   * time limit.  If the future is not marked as done by the time limit then it will be 
   * completed by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link WatchdogCache} was constructed.
   * 
   * @param future Future to inspect to ensure completion
   * @param timeoutInMillis Time in milliseconds that future should be completed within
   */
  public void watch(ListenableFuture<?> future, long timeoutInMillis) {
    // attempt around a cheap shortcut
    if (future == null || future.isDone()) {
      return;
    }
    
    cachedDogs.computeIfAbsent(timeoutInMillis, watchdogProducer)
              .watch(future);
    
    maybeScheduleCleaner();
  }
  
  private void maybeScheduleCleaner() {
    if (! cleanerScheduled.get() && cleanerScheduled.compareAndSet(false, true)) {
      scheduler.schedule(cacheCleaner, INSPECTION_INTERVAL_MILLIS);
    }
  }
  
  /**
   * <p>Runnable which looks over all cached {@link Watchdog} instances to see if any are no 
   * longer active.  Removing the inactive ones as they are found.  This also handles rescheduling 
   * itself if future inspection may be needed.</p>
   * 
   * @author jent - Mike Jensen
   * @since 4.0.0
   */
  private class CleanRunner implements Runnable {
    @Override
    public void run() {
      Iterator<Watchdog> it = cachedDogs.values().iterator();
      while (it.hasNext()) {
        if (! it.next().isActive()) {
          it.remove();
        }
      }
      
      // must set unscheduled before checking if we need to reschedule
      cleanerScheduled.set(false);
      
      if (! cachedDogs.isEmpty()) {
        maybeScheduleCleaner();
      }
    }
  }
}
