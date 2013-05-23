package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.Clock;

/**
 * This is a small wrapper class for Clock to avoid updating for bulk calls.
 * This is primarily useful for the PriorityScheduledExecutor when it needs to 
 * do a binary search in a list of delayed items, or sort those delayed items.
 * There is no reason to make a system call for each item, so we just pause getting
 * accurate time for those fast but frequent operations.
 * 
 * @author jent - Mike Jensen
 */
public class ClockWrapper {
  private static AtomicInteger requestsToStopUpdatingTime = new AtomicInteger();
  
  /**
   * A call here causes getAccurateTime to use the last known time.
   */
  protected static void stopForcingUpdate() {
    requestsToStopUpdatingTime.incrementAndGet();
  }
  
  /**
   * This resumes updating the clock for calls to getAccurateTime.
   */
  protected static void resumeForcingUpdate() {
    int newVal = requestsToStopUpdatingTime.decrementAndGet();
    
    if (newVal < 0) {
      boolean ableToCorrect = requestsToStopUpdatingTime.compareAndSet(newVal, 0);
      throw new IllegalStateException("Should have never become negative...corrected: " + ableToCorrect);
    }
  }
  
  /**
   * Returns an accurate time based on if it has been requested to 
   * stop updating from system clock temporarily or not.
   */
  protected static long getAccurateTime() {
    if (requestsToStopUpdatingTime.get() > 0) {
      return Clock.lastKnownTimeMillis();
    } else {
      return Clock.accurateTime();
    }
  }
  
  /**
   * Forces an update to the clock.
   * 
   * @return the current time in millis
   */
  protected static long updateClock() {
    return Clock.accurateTime();
  }
  
  /**
   * @return the last stored time in millis
   */
  protected static long getLastKnownTime() {
    return Clock.lastKnownTimeMillis();
  }
}
