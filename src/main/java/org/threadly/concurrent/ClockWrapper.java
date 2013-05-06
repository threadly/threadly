package org.threadly.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.threadly.util.Clock;

/**
 * @author jent - Mike Jensen
 */
public class ClockWrapper {
  private static final Logger log = Logger.getLogger(ClockWrapper.class.getSimpleName());
  
  private static AtomicInteger requestsToStopUpdatingTime = new AtomicInteger();
  
  protected static void stopForcingUpdate() {
    requestsToStopUpdatingTime.incrementAndGet();
  }
  
  protected static void resumeForcingUpdate() {
    int newVal = requestsToStopUpdatingTime.decrementAndGet();
    
    while (newVal < 0 && ! requestsToStopUpdatingTime.compareAndSet(newVal, 0)) {
      log.warning("requestsToStopUpdatingTime < 0 (" + newVal + ")...resetting");
      newVal = requestsToStopUpdatingTime.get();
    }
  }
  
  protected static long getAccurateTime() {
    if (requestsToStopUpdatingTime.get() > 0) {
      return Clock.lastKnownTimeMillis();
    } else {
      return Clock.accurateTime();
    }
  }
  
  protected static long updateClock() {
    return Clock.accurateTime();
  }
  
  protected static long getLastKnownTime() {
    return Clock.lastKnownTimeMillis();
  }
}
