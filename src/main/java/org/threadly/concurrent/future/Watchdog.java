package org.threadly.concurrent.future;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.watchdog.ConstantTimeWatchdog;

/**
 * This class is to guarantee that a given {@link ListenableFuture} is completed within a 
 * timeout.  Once the timeout is reached, if the future has not already completed this will 
 * attempt to invoke {@link ListenableFuture#cancel(boolean)}.  The future should then throw a 
 * {@link java.util.concurrent.CancellationException} on a {@link ListenableFuture#get()} call.
 * <p>
 * Using {@link CancelDebuggingListenableFuture} to wrap the futures before providing to this class 
 * can provide an easier understanding of the state of a Future when it was timed out by this class.
 * 
 * @deprecated Moved and renamed to {@link ConstantTimeWatchdog}
 * 
 * @since 4.0.0
 */
@Deprecated
public class Watchdog extends ConstantTimeWatchdog {
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
    super(timeoutInMillis, sendInterruptOnFutureCancel);
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
    super(scheduler, timeoutInMillis, sendInterruptOnFutureCancel);
  }
}
