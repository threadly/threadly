package org.threadly.concurrent.future.watchdog;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;

/**
 * A class which handles a collection of {@link ConstantTimeWatchdog} instances.  Because the 
 * timeout for {@link ConstantTimeWatchdog} is set in the constructor 
 * {@link ConstantTimeWatchdog#ConstantTimeWatchdog(long, boolean)}, you can 
 * use this class to be more flexible and set the timeout at the time of watching the future.
 * <p>
 * Using {@link org.threadly.concurrent.future.CancelDebuggingListenableFuture} to wrap the futures 
 * before providing to this class can provide an easier understanding of the state of a Future when 
 * it was timed out by this class.
 * 
 * @since 5.40 (existed since 4.0.0 as org.threadly.concurrent.future.WatchdogCache)
 */
public class MixedTimeWatchdog {
  protected static final int INSPECTION_INTERVAL_MILLIS = 10_000;
  protected static final int DEFAULT_RESOLUTION_MILLIS = 200;

  private static final AtomicReference<MixedTimeWatchdog> INTERRUPTING_WATCHDOG_CACHE = 
      new AtomicReference<>();
  private static final AtomicReference<MixedTimeWatchdog> NONINTERRUPTING_WATCHDOG_CACHE = 
      new AtomicReference<>();
  
  /**
   * Return a static / shared {@link MixedTimeWatchdog} instance.  This instance is backed by the 
   * {@link org.threadly.concurrent.CentralThreadlyPool} which should be fine in most cases, but if 
   * you have specific needs you can construct your own instance by 
   * {@link #MixedTimeWatchdog(SubmitterScheduler, boolean)}, or if you need to specify a 
   * specific timeout resolution using the 
   * {@link #MixedTimeWatchdog(SubmitterScheduler, boolean, long)} constructor.
   * <p>
   * As long as those special cases are not needed, using a shared instance allows for potentially 
   * improved efficiency.
   * 
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   * @return A shared {@link MixedTimeWatchdog} with the specified configuration
   */
  public static final MixedTimeWatchdog centralWatchdog(boolean sendInterruptOnFutureCancel) {
    AtomicReference<MixedTimeWatchdog> ar = 
        sendInterruptOnFutureCancel ? INTERRUPTING_WATCHDOG_CACHE : NONINTERRUPTING_WATCHDOG_CACHE;
    MixedTimeWatchdog wd = ar.get();
    if (wd == null) {
      ar.compareAndSet(null, new MixedTimeWatchdog(AbstractWatchdog.getStaticScheduler(), 
                                                      sendInterruptOnFutureCancel));
      wd = ar.get();
    }
    
    return wd;
  }
  
  protected final SubmitterScheduler scheduler;
  protected final boolean sendInterruptOnFutureCancel;
  protected final ConcurrentMap<Long, ConstantTimeWatchdog> cachedDogs;
  protected final Function<Long, ConstantTimeWatchdog> watchdogProducer;
  protected final Runnable cacheCleaner;
  protected final long resolutionMillis;
  private final AtomicBoolean cleanerScheduled;

  /**
   * Constructs a new {@link MixedTimeWatchdog} with a scheduler of your choosing.  It is critical 
   * that this scheduler has a free thread available to inspect futures which may not have 
   * completed in the given timeout.  You may want to use a org.threadly.concurrent.limiter to 
   * ensure that there are threads available.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public MixedTimeWatchdog(SubmitterScheduler scheduler, boolean sendInterruptOnFutureCancel) {
    this(scheduler, sendInterruptOnFutureCancel, DEFAULT_RESOLUTION_MILLIS);
  }

  /**
   * Constructs a new {@link MixedTimeWatchdog} with a scheduler of your choosing.  It is critical 
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
  public MixedTimeWatchdog(SubmitterScheduler scheduler, boolean sendInterruptOnFutureCancel, 
                           long resolutionMillis) {
    ArgumentVerifier.assertGreaterThanZero(resolutionMillis, "resolutionMillis");
    
    this.scheduler = scheduler;
    this.sendInterruptOnFutureCancel = sendInterruptOnFutureCancel;
    cachedDogs = new ConcurrentHashMap<>();
    watchdogProducer = (timeout) -> {
      maybeScheduleCleaner();
      return new ConstantTimeWatchdog(scheduler, timeout, sendInterruptOnFutureCancel);
    };
    cacheCleaner = new CleanRunner();
    cleanerScheduled = new AtomicBoolean(false);
    this.resolutionMillis = resolutionMillis;
  }

  /**
   * Check how many futures are currently being monitored for completion.
   * 
   * @return The number of futures being monitored
   */
  public int getWatchingCount() {
    int result = 0;
    for (ConstantTimeWatchdog wd : cachedDogs.values()) {
      result += wd.getWatchingCount();
    }
    return result;
  }
  
  /**
   * Watch a given {@link ListenableFuture} to ensure that it completes within the provided 
   * time limit.  If the future is not marked as done by the time limit then it will be 
   * completed by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link MixedTimeWatchdog} was constructed.
   * 
   * @param timeoutInMillis Time in milliseconds that future should be completed within
   * @param future Future to inspect to ensure completion
   */
  public void watch(long timeoutInMillis, ListenableFuture<?> future) {
    long adjustedTimeout = timeoutInMillis / resolutionMillis; // int division to zero out to resolution
    adjustedTimeout *= resolutionMillis;
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
   * @since 5.40
   */
  private class CleanRunner implements Runnable {
    @Override
    public void run() {
      try {
        Iterator<ConstantTimeWatchdog> it = cachedDogs.values().iterator();
        while (it.hasNext()) {
          if (! it.next().isActive()) {
            it.remove();
          }
        }
      } finally {
        if (cachedDogs.isEmpty()) {
          cleanerScheduled.set(false);
          // must re-check state to ensure no concurrent Watchdog was added
          if (! cachedDogs.isEmpty()) {
            maybeScheduleCleaner();
          }
        } else {
          scheduler.schedule(this, INSPECTION_INTERVAL_MILLIS);
        }
      }
    }
  }
}
