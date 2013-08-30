package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.Clock;

/**
 * This is a small wrapper class for Clock to avoid updating for bulk calls.
 * This is primarily useful for the {@link PriorityScheduledExecutor} when it needs to 
 * do a binary search in a list of delayed items, or sort those delayed items.
 * There is no reason to make a system call for each item, so we just pause getting
 * accurate time for those fast but frequent operations.
 * 
 * All the functions in this class are protected because it is not intended to be used 
 * outside of this package.  This is a utility class that must be handled carefully, 
 * using it incorrectly could have serious impacts on other classes which depend on it.
 * 
 * @author jent - Mike Jensen
 */
class ClockWrapper {
  private static final AtomicInteger REQUESTS_TO_STOP_UPDATING_TIME = new AtomicInteger();
  
  private ClockWrapper() {
    // don't construct
  }
  
  /**
   * A call here causes getAccurateTime to use the last known time.
   */
  protected static void stopForcingUpdate() {
    REQUESTS_TO_STOP_UPDATING_TIME.incrementAndGet();
  }
  
  /**
   * This resumes updating the clock for calls to getAccurateTime.
   */
  protected static void resumeForcingUpdate() {
    int newVal = REQUESTS_TO_STOP_UPDATING_TIME.decrementAndGet();
    
    if (newVal < 0) {
      boolean ableToCorrect = REQUESTS_TO_STOP_UPDATING_TIME.compareAndSet(newVal, 0);
      throw new IllegalStateException("Should have never become negative...corrected: " + ableToCorrect);
    }
  }
  
  /**
   * Returns an accurate time based on if it has been requested to 
   * stop updating from system clock temporarily or not.
   */
  protected static long getAccurateTime() {
    if (REQUESTS_TO_STOP_UPDATING_TIME.get() > 0) {
      return Clock.lastKnownTimeMillis();
    } else {
      return Clock.accurateTime();
    }
  }
  
  /**
   * Forces an update to the clock, regardless of requests to stop forcing updates.
   * 
   * @return the current time in milliseconds
   */
  protected static long updateClock() {
    return Clock.accurateTime();
  }
  
  /**
   * Call to get a semi-accurate time, based off the last time
   * clock has updated, or accurate time has been requested.
   * 
   * @return the last stored time in milliseconds
   */
  protected static long getLastKnownTime() {
    return Clock.lastKnownTimeMillis();
  }
}
