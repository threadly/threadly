package org.threadly.concurrent.future;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.util.ArgumentVerifier;

/**
 * A class which handles a collection of  {@link Watchdog} instances.  Because the timeout for 
 * {@link Watchdog} is set in the constructor {@link Watchdog#Watchdog(long, boolean)}, you can 
 * use this class to be more flexible and set the timeout at the time of watching the future.
 * <p>
 * Using {@link CancelDebuggingListenableFuture} to wrap the futures before providing to this class 
 * can provide an easier understanding of the state of a Future when it was timed out by this class.
 * 
 * @since 4.0.0
 */
public class WatchdogCache {
  protected static final int INSPECTION_INTERVAL_MILLIS = 10_000;
  protected static final int DEFAULT_RESOLUTION_MILLIS = 200;

  private static final AtomicReference<WatchdogCache> INTERRUPTING_WATCHDOG_CACHE = 
      new AtomicReference<>();
  private static final AtomicReference<WatchdogCache> NONINTERRUPTING_WATCHDOG_CACHE = 
      new AtomicReference<>();
  
  /**
   * Return a static / shared {@link WatchdogCache} instance.  This instance is backed by the 
   * {@link org.threadly.concurrent.CentralThreadlyPool} which should be fine in most cases, but if 
   * you have specific needs you can construct your own instance by 
   * {@link #WatchdogCache(SubmitterScheduler, boolean)}, or if you need to specify a specific 
   * timeout resolution using the {@link #WatchdogCache(SubmitterScheduler, boolean, long)} 
   * constructor.
   * <p>
   * As long as those special cases are not needed, using a shared instance allows for potentially 
   * improved efficiency.
   * 
   * @since 5.19
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   * @return A shared {@link WatchdogCache} with the specified configuration
   */
  public static final WatchdogCache centralWatchdogCache(boolean sendInterruptOnFutureCancel) {
    AtomicReference<WatchdogCache> ar = sendInterruptOnFutureCancel ? 
                                          INTERRUPTING_WATCHDOG_CACHE : NONINTERRUPTING_WATCHDOG_CACHE;
    WatchdogCache wd = ar.get();
    if (wd == null) {
      ar.compareAndSet(null, new WatchdogCache(Watchdog.getStaticScheduler(), 
                                               sendInterruptOnFutureCancel));
      wd = ar.get();
    }
    
    return wd;
  }
  
  protected final SubmitterScheduler scheduler;
  protected final boolean sendInterruptOnFutureCancel;
  protected final ConcurrentMap<Long, Watchdog> cachedDogs;
  protected final Function<Long, Watchdog> watchdogProducer;
  protected final Runnable cacheCleaner;
  protected final long resolutionMillis;
  private final AtomicBoolean cleanerScheduled;
  
  /**
   * Constructs a new {@link WatchdogCache}.  This constructor will use a default static scheduler 
   * (which is lazily constructed).  This should be fine in most cases, but you can provide your 
   * own scheduler if you want to avoid the thread creation (which is shared among all instances 
   * that were constructed with this constructor or {@link Watchdog#Watchdog(long, boolean)}}.
   * 
   * @deprecated Please use {@link #centralWatchdogCache(boolean)}
   * 
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  @Deprecated
  public WatchdogCache(boolean sendInterruptOnFutureCancel) {
    this(Watchdog.getStaticScheduler(), sendInterruptOnFutureCancel, DEFAULT_RESOLUTION_MILLIS);
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
    this(scheduler, sendInterruptOnFutureCancel, DEFAULT_RESOLUTION_MILLIS);
  }

  /**
   * Constructs a new {@link WatchdogCache} with a scheduler of your choosing.  It is critical 
   * that this scheduler has a free thread available to inspect futures which may not have 
   * completed in the given timeout.  You may want to use a org.threadly.concurrent.limiter to 
   * ensure that there are threads available.
   * <p>
   * This constructor allows you to set the timeout resolutions.  Setting the resolution too large
   * can result in futures timing out later than you expected.  Setting it too low results in 
   * heavy memory consumption when used with a wide variety of timeouts.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   * @param resolutionMillis The resolution to allow timeout granularity
   */
  public WatchdogCache(SubmitterScheduler scheduler, boolean sendInterruptOnFutureCancel, 
                       long resolutionMillis) {
    ArgumentVerifier.assertGreaterThanZero(resolutionMillis, "resolutionMillis");
    
    this.scheduler = scheduler;
    this.sendInterruptOnFutureCancel = sendInterruptOnFutureCancel;
    cachedDogs = new ConcurrentHashMap<>();
    watchdogProducer = (timeout) -> {
      maybeScheduleCleaner();
      return new Watchdog(scheduler, timeout, sendInterruptOnFutureCancel);
    };
    cacheCleaner = new CleanRunner();
    cleanerScheduled = new AtomicBoolean(false);
    this.resolutionMillis = resolutionMillis;
  }
  
  /**
   * Watch a given {@link ListenableFuture} to ensure that it completes within the provided 
   * time limit.  If the future is not marked as done by the time limit then it will be 
   * completed by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link WatchdogCache} was constructed.
   * 
   * @deprecated Please re-order arguments to use {@link #watch(long, ListenableFuture)}
   * 
   * @param future Future to inspect to ensure completion
   * @param timeoutInMillis Time in milliseconds that future should be completed within
   */
  @Deprecated
  public void watch(ListenableFuture<?> future, long timeoutInMillis) {
    watch(timeoutInMillis, future);
  }
  
  /**
   * Watch a given {@link ListenableFuture} to ensure that it completes within the provided 
   * time limit.  If the future is not marked as done by the time limit then it will be 
   * completed by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link WatchdogCache} was constructed.
   * 
   * @param timeoutInMillis Time in milliseconds that future should be completed within
   * @param future Future to inspect to ensure completion
   */
  public void watch(long timeoutInMillis, ListenableFuture<?> future) {
    long adjustedTimeout = timeoutInMillis / resolutionMillis;
    adjustedTimeout *= resolutionMillis; // int division to zero out
    if (adjustedTimeout != timeoutInMillis) {
      adjustedTimeout += resolutionMillis;  // prefer timing out later rather than early
    }
    
    // attempt around a cheap shortcut
    if (future == null || future.isDone()) {
      return;
    }
    
    cachedDogs.computeIfAbsent(adjustedTimeout, watchdogProducer)
              .watch(future);
  }
  
  private void maybeScheduleCleaner() {
    if (! cleanerScheduled.get() && cleanerScheduled.compareAndSet(false, true)) {
      scheduler.schedule(cacheCleaner, INSPECTION_INTERVAL_MILLIS);
    }
  }
  
  /**
   * Runnable which looks over all cached {@link Watchdog} instances to see if any are no longer 
   * active.  Removing the inactive ones as they are found.  This also handles rescheduling itself 
   * if future inspection may be needed.
   * 
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
