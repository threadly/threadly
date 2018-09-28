package org.threadly.concurrent.future;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * This class is to guarantee that a given {@link ListenableFuture} is completed within a 
 * timeout.  Once the timeout is reached, if the future has not already completed this will 
 * attempt to invoke {@link ListenableFuture#cancel(boolean)}.  The future should then throw a 
 * {@link java.util.concurrent.CancellationException} on a {@link ListenableFuture#get()} call.
 * <p>
 * Using {@link CancelDebuggingListenableFuture} to wrap the futures before providing to this class 
 * can provide an easier understanding of the state of a Future when it was timed out by this class.
 * 
 * @since 4.0.0
 */
public class Watchdog {
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
  
  protected final long timeoutInMillis;
  protected final boolean sendInterruptToTrackedThreads;
  protected final CheckRunner checkRunner;
  protected final Queue<FutureWrapper> futures;
  
  /**
   * Constructs a new {@link Watchdog}.  This constructor will use a default static scheduler 
   * (which is lazily constructed).  This should be fine in most cases, but you can provide your 
   * own scheduler if you have specific needs where the {@link CentralThreadlyPool} default is not 
   * a good option.
   * 
   * @param timeoutInMillis Time in milliseconds that futures will be set to error if they are not done
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public Watchdog(long timeoutInMillis, boolean sendInterruptOnFutureCancel) {
    this(getStaticScheduler(), timeoutInMillis, sendInterruptOnFutureCancel);
  }
  
  /**
   * Constructs a new {@link Watchdog} with a scheduler of your choosing.  It is critical that 
   * this scheduler has a free thread available to inspect futures which may not have completed in 
   * the given timeout.  You may want to use a org.threadly.concurrent.limiter to ensure that 
   * there are threads available.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param timeoutInMillis Time in milliseconds that futures will be set to error if they are not done
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public Watchdog(SubmitterScheduler scheduler, long timeoutInMillis, 
                  boolean sendInterruptOnFutureCancel) {
    // scheduler not null verified in CheckRunner
    ArgumentVerifier.assertGreaterThanZero(timeoutInMillis, "timeoutInMillis");
    
    this.timeoutInMillis = timeoutInMillis;
    this.sendInterruptToTrackedThreads = sendInterruptOnFutureCancel;
    this.checkRunner = new CheckRunner(scheduler, timeoutInMillis);
    this.futures = new ConcurrentLinkedQueue<>();
  }
  
  /**
   * Request the timeout in milliseconds until futures that have not completed are canceled.  This 
   * is the timeout that the class was constructed with (since it can not be changed after 
   * construction).
   * 
   * @return Time in milliseconds till incomplete futures have {@link ListenableFuture#cancel(boolean)} invoked
   */
  public long getTimeoutInMillis() {
    return timeoutInMillis;
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
   * Watch a given {@link ListenableFuture} to ensure that it completes within the constructed 
   * time limit.  If the future is not marked as done by the time limit then it will be completed 
   * by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link Watchdog} was constructed.
   * 
   * @param future Future to inspect to ensure completion
   */
  public void watch(ListenableFuture<?> future) {
    if (future == null || future.isDone()) {
      return;
    }
    
    final FutureWrapper fw = new FutureWrapper(future);
    futures.add(fw);
    // we attempt to remove the future on completion to reduce inspection needed
    future.addListener(new WrapperRemover(fw), SameThreadSubmitterExecutor.instance());
    
    checkRunner.signalToRun();
  }

  /**
   * Just a simple wrapper class so we can hold not just the future, but what time the future will 
   * expire at.
   * 
   * @since 4.0.0
   */
  private class FutureWrapper {
    public final long expireTime;
    private final ListenableFuture<?> future;

    public FutureWrapper(ListenableFuture<?> future) {
      this.expireTime = Clock.accurateForwardProgressingMillis() + timeoutInMillis;
      this.future = future;
    }
  }
  
  /**
   * Listener implementation for removing the wrapper from the queue when it completes (and thus 
   * invokes this).
   * 
   * @since 5.20
   */
  private class WrapperRemover implements Runnable {
    private final FutureWrapper fw;
    
    protected WrapperRemover(FutureWrapper fw) {
      this.fw = fw;
    }
    
    @Override
    public void run() {
      futures.remove(fw);
    }
  }

  /**
   * This runnable inspects over the queue looking for futures which have expired and need to be 
   * canceled.  It may reschedule itself if it is not able to fully examine the queue (because not 
   * all items are currently ready for inspection).
   * 
   * @since 4.0.0
   */
  protected class CheckRunner extends ReschedulingOperation {
    public CheckRunner(SubmitterScheduler scheduler, long scheduleDelay) {
      super(scheduler, scheduleDelay);
    }

    @Override
    protected void run() {
      long now = Clock.accurateForwardProgressingMillis();
      Iterator<FutureWrapper> it = futures.iterator();
      FutureWrapper fw = null;
      while (it.hasNext()) {
        fw = it.next();
        if (now >= fw.expireTime || (now = Clock.accurateForwardProgressingMillis()) >= fw.expireTime) {
          it.remove();
          fw.future.cancel(sendInterruptToTrackedThreads);
          fw = null;
        } else {
          /* since futures are added in order of expiration, 
          we know at this point we don't need to inspect any more items*/
          break;
        }
      }
      
      if (fw != null) {
        // update our execution time to when the next expiration will occur
        setScheduleDelay(fw.expireTime - now);
        signalToRun();  // notify we still have work to do
      } else {
        // ensure schedule delay is set correctly
        setScheduleDelay(timeoutInMillis);
      }
    }
  }
}
