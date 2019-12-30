package org.threadly.concurrent.future.watchdog;

import java.util.Collection;
import java.util.Iterator;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.Clock;

/**
 * This class is to guarantee that a given {@link ListenableFuture} is completed within a 
 * timeout.  Once the timeout is reached, if the future has not already completed this will 
 * attempt to invoke {@link ListenableFuture#cancel(boolean)}.  The future should then throw a 
 * {@link java.util.concurrent.CancellationException} on a {@link ListenableFuture#get()} call.
 * <p>
 * Using {@link org.threadly.concurrent.future.CancelDebuggingListenableFuture} to wrap the futures 
 * before providing to this class can provide an easier understanding of the state of a Future when 
 * it was timed out by this class.
 * 
 * @since 5.40 (existed since 4.0.0 as org.threadly.concurrent.future.Watchdog)
 */
public class ConstantTimeWatchdog extends AbstractWatchdog {
  protected final long timeoutInMillis;
  
  /**
   * Constructs a new {@link ConstantTimeWatchdog}.  This constructor will use a default static scheduler 
   * (which is lazily constructed).  This should be fine in most cases, but you can provide your 
   * own scheduler if you have specific needs where the {@link CentralThreadlyPool} default is not 
   * a good option.
   * 
   * @param timeoutInMillis Time in milliseconds that futures will be set to error if they are not done
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public ConstantTimeWatchdog(long timeoutInMillis, boolean sendInterruptOnFutureCancel) {
    this(getStaticScheduler(), timeoutInMillis, sendInterruptOnFutureCancel);
  }
  
  /**
   * Constructs a new {@link ConstantTimeWatchdog} with a scheduler of your choosing.  It is critical that 
   * this scheduler has a free thread available to inspect futures which may not have completed in 
   * the given timeout.  You may want to use a org.threadly.concurrent.limiter to ensure that 
   * there are threads available.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param timeoutInMillis Time in milliseconds that futures will be set to error if they are not done
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  @SuppressWarnings("unchecked")
  public ConstantTimeWatchdog(SubmitterScheduler scheduler, long timeoutInMillis, 
                              boolean sendInterruptOnFutureCancel) {
    super((futures) -> new CheckRunner(scheduler, timeoutInMillis, 
                                       sendInterruptOnFutureCancel, 
                                       (Collection<TimeoutFutureWrapper>) futures));
    
    // scheduler not null verified in CheckRunner
    ArgumentVerifier.assertGreaterThanZero(timeoutInMillis, "timeoutInMillis");
    
    this.timeoutInMillis = timeoutInMillis;
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
   * Watch a given {@link ListenableFuture} to ensure that it completes within the constructed 
   * time limit.  If the future is not marked as done by the time limit then it will be completed 
   * by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link ConstantTimeWatchdog} was constructed.
   * 
   * @param future Future to inspect to ensure completion
   */
  public void watch(ListenableFuture<?> future) {
    if (future == null || future.isDone()) {
      return;
    }

    watchWrapper(new TimeoutFutureWrapper(future), future);
  }

  /**
   * Just a simple wrapper class so we can hold not just the future, but what time the future will 
   * expire at.
   * 
   * @since 5,40
   */
  protected class TimeoutFutureWrapper {
    public final long expireTime;
    protected final ListenableFuture<?> future;

    public TimeoutFutureWrapper(ListenableFuture<?> future) {
      this.expireTime = Clock.accurateForwardProgressingMillis() + timeoutInMillis;
      this.future = future;
    }
  }

  /**
   * This runnable inspects over the queue looking for futures which have expired and need to be 
   * canceled.  It may reschedule itself if it is not able to fully examine the queue (because not 
   * all items are currently ready for inspection).
   * 
   * @since 5.40
   */
  protected static class CheckRunner extends ReschedulingOperation {
    protected final long timeoutInMillis;
    protected final boolean sendInterruptToTrackedThreads;
    protected final Collection<TimeoutFutureWrapper> futures;
    
    public CheckRunner(SubmitterScheduler scheduler, long timeoutInMillis, 
                       boolean sendInterruptToTrackedThreads, 
                       Collection<TimeoutFutureWrapper> futures) {
      super(scheduler, timeoutInMillis);
      
      this.timeoutInMillis = timeoutInMillis;
      this.sendInterruptToTrackedThreads = sendInterruptToTrackedThreads;
      this.futures = futures;
    }

    @Override
    protected void run() {
      TimeoutFutureWrapper fw = null;
      Iterator<TimeoutFutureWrapper> it = futures.iterator();
      long now = -1;  // set negative to force refresh on first check
      while (it.hasNext()) {
        fw = it.next();
        if (now >= fw.expireTime || 
            (now = Clock.accurateForwardProgressingMillis()) >= fw.expireTime) {
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
