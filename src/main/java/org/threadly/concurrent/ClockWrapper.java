package org.threadly.concurrent;

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
  protected static final ThreadLocal<ClockWrapperState> CLOCK_STATE;

  static {
    CLOCK_STATE = new ThreadLocal<ClockWrapperState>() {
      @Override
      protected ClockWrapperState initialValue() {
        return new ClockWrapperState();
      }
    };
  }
  
  /**
   * A call here causes {@link #getSemiAccurateMillis()} to use the last known time.  If this is the 
   * first call to stop updating the time, it will ensure the clock is updated first.
   */
  protected static void stopForcingUpdate() {
    ClockWrapperState state = CLOCK_STATE.get();
    // if we are the first one to increment, we need to set the lastKnownTime
    if (state.requestsToStopUpdatingTime++ == 0) {
      state.lastKnownTime = Clock.accurateForwardProgressingMillis();
    }
  }
  
  /**
   * This resumes updating the clock for calls to {@link #getSemiAccurateMillis()}.
   */
  protected static void resumeForcingUpdate() {
    CLOCK_STATE.get().requestsToStopUpdatingTime--;
  }
  
  /**
   * Returns an accurate time based on if it has been requested to stop updating from system clock 
   * temporarily or not.
   */
  protected static long getSemiAccurateMillis() {
    ClockWrapperState state = CLOCK_STATE.get();
    if (state.requestsToStopUpdatingTime > 0) {
      return state.lastKnownTime;
    } else {
      return Clock.accurateForwardProgressingMillis();
    }
  }
  
  /**
   * <p>This class tracks the quantity of requests to stop updating, as well as what the time used 
   * for reference if there are requests to stop using it.  This is NOT thread safe, which is 
   * due to this expecting to be used as a ThreadLocal storage</p>
   * 
   * @author jent - Mike Jensen
   * @since 3.4.0
   */
  private static class ClockWrapperState {
    private int requestsToStopUpdatingTime = 0;
    private long lastKnownTime;
  }
}
