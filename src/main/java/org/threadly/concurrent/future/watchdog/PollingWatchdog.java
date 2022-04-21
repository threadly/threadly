package org.threadly.concurrent.future.watchdog;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;

import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.ReschedulingOperation;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.util.ArgumentVerifier;
import org.threadly.util.ExceptionUtils;

/**
 * This class is designed to help ensure a future completes with an arbitrary condition to 
 * determine when a future should be canceled.  If the condition is time based then see 
 * {@link ConstantTimeWatchdog} and {@link MixedTimeWatchdog} as possibly more efficient 
 * options.  This class however can allow the condition to be independent from the submission to be 
 * watched.  For example if you want to time something out after a lack of progress, but don't care 
 * about the absolute time it has been running you can track that progress and provide a 
 * {@link Supplier} to {@link #watch(Supplier, ListenableFuture)} which will return {@code true} 
 * only once there has been no progress in X milliseconds.  The supplier will be invoked 
 * regularly to check the state associated to the matching future.
 * 
 * @since 5.40
 */
public class PollingWatchdog extends AbstractWatchdog {
  /**
   * Constructs a new {@link PollingWatchdog}.  This constructor will use a default static scheduler 
   * (which is lazily constructed).  This should be fine in most cases, but you can provide your 
   * own scheduler if you have specific needs where the {@link CentralThreadlyPool} default is not 
   * a good option.
   * 
   * @param pollFrequencyMillis Time in milliseconds that futures will be checked if they should be timed out
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  public PollingWatchdog(long pollFrequencyMillis, boolean sendInterruptOnFutureCancel) {
    this(getStaticScheduler(), pollFrequencyMillis, sendInterruptOnFutureCancel);
  }
  
  /**
   * Constructs a new {@link PollingWatchdog} with a scheduler of your choosing.  It is critical that 
   * this scheduler has a free thread available to inspect futures which may not have completed in 
   * the given timeout.  You may want to use a org.threadly.concurrent.limiter to ensure that 
   * there are threads available.
   * 
   * @param scheduler Scheduler to schedule task to look for expired futures
   * @param pollFrequencyMillis Time in milliseconds that futures will be checked if they should be timed out
   * @param sendInterruptOnFutureCancel If {@code true}, and a thread is provided with the future, 
   *                                      an interrupt will be sent on timeout
   */
  @SuppressWarnings("unchecked")
  public PollingWatchdog(SubmitterScheduler scheduler, long pollFrequencyMillis, 
                         boolean sendInterruptOnFutureCancel) {
    super((futures) -> new CheckRunner(scheduler, pollFrequencyMillis, 
                                       sendInterruptOnFutureCancel, 
                                       (Collection<PollingFutureWrapper>) futures));
    
    // scheduler not null verified in CheckRunner
    ArgumentVerifier.assertGreaterThanZero(pollFrequencyMillis, "pollFrequencyMillis");
  }

  /**
   * Watch a given {@link ListenableFuture} to ensure that it completes within the constructed 
   * time limit.  If the future is not marked as done by the time limit then it will be completed 
   * by invoking {@link ListenableFuture#cancel(boolean)}.  Weather a {@code true} or 
   * {@code false} will be provided to interrupt the running thread is dependent on how this 
   * {@link PollingWatchdog} was constructed.
   * <p>
   * The provided {@link Supplier} will be invoked regularly as the state is polled.  Once it 
   * returns {@code true} it will never be invoked again, and instead 
   * {@link ListenableFuture#cancel(boolean)} will be invoked on the provided future. 
   * 
   * @param cancelTest Supplier to return {@code true} when the future should be canceled
   * @param future Future to inspect to ensure completion
   */
  public void watch(Supplier<Boolean> cancelTest, ListenableFuture<?> future) {
    if (future == null || future.isDone()) {
      return;
    }
    
    watchWrapper(new PollingFutureWrapper(cancelTest, future), future);
  }

  /**
   * Just a simple wrapper class so we can hold not just the future, but what time the future will 
   * expire at.
   * 
   * @since 5.40
   */
  protected class PollingFutureWrapper {
    protected final Supplier<Boolean> cancelTest;
    protected final ListenableFuture<?> future;

    public PollingFutureWrapper(Supplier<Boolean> cancelTest, ListenableFuture<?> future) {
      this.cancelTest = cancelTest;
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
    protected final boolean sendInterruptToTrackedThreads;
    protected final Collection<PollingFutureWrapper> futures;
    
    public CheckRunner(SubmitterScheduler scheduler, long scheduleDelay, 
                       boolean sendInterruptToTrackedThreads, 
                       Collection<PollingFutureWrapper> futures) {
      super(scheduler, scheduleDelay);
      
      this.sendInterruptToTrackedThreads = sendInterruptToTrackedThreads;
      this.futures = futures;
    }

    @Override
    protected void run() {
      PollingFutureWrapper fw = null;
      Iterator<PollingFutureWrapper> it = futures.iterator();
      while (it.hasNext()) {
        try {
          fw = it.next();
          if (fw.future.isDone() || fw.cancelTest.get()) {
            it.remove();
            fw.future.cancel(sendInterruptToTrackedThreads);
          }
        } catch (Throwable t) {
          ExceptionUtils.handleException(t);
        }
      }
      
      if (! futures.isEmpty()) {
        signalToRun();  // notify we still have work to do
      }
    }
  }
}
