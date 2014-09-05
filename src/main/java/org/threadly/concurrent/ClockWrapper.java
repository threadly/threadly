package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.threadly.util.Clock;

/**
 * <p>This is a small wrapper class for Clock to avoid updating for bulk calls.  This is primarily 
 * useful for the {@link PriorityScheduler} when it needs to do a binary search in a list of 
 * delayed items, or sort those delayed items.  There is no reason to make a system call for each 
 * item, so we just pause getting accurate time for those fast but frequent operations.</p>
 * 
 * <p>In addition because the {@link Clock} class may jump large amounts of time as it is updated, 
 * this class attempts to return a very similar amount of time after {@link #stopForcingUpdate()} 
 * has been called.  This means that getSemiAccurateTime may be less accurate than the {@link Clock} 
 * representation in order to ensure consistency.</p>
 * 
 * <p>All the functions in this class are protected because it is not intended to be used outside 
 * of this package.  This is a utility class that must be handled carefully, using it incorrectly 
 * could have serious impacts on other classes which depend on it.</p>
 * 
 * @author jent - Mike Jensen
 * @since 1.0.0
 */
class ClockWrapper {
  protected final AtomicInteger requestsToStopUpdatingTime;
  private volatile long lastKnownTime = -1;
  
  protected ClockWrapper() {
    requestsToStopUpdatingTime = new AtomicInteger();
    lastKnownTime = -1;
  }
  
  /**
   * A call here causes {@link #getSemiAccurateTime()} to use the last known time.  If this is the 
   * first call to stop updating the time, it will ensure the clock is updated first.
   */
  protected void stopForcingUpdate() {
    if (requestsToStopUpdatingTime.getAndIncrement() == 0) {
      lastKnownTime = Clock.accurateTimeMillis();
    }
  }
  
  /**
   * This resumes updating the clock for calls to {@link #getSemiAccurateTime()}.
   */
  protected void resumeForcingUpdate() {
    int newVal = requestsToStopUpdatingTime.decrementAndGet();
    
    if (newVal < 0) {
      throw new IllegalStateException();
    }
  }
  
  /**
   * Returns an accurate time based on if it has been requested to stop updating from system clock 
   * temporarily or not.
   */
  protected long getSemiAccurateTime() {
    if (requestsToStopUpdatingTime.get() > 0) {
      return lastKnownTime;
    } else {
      return Clock.accurateTimeMillis();
    }
  }
}
