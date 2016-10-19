package org.threadly.test.concurrent;

import org.threadly.util.Clock;

/**
 * Generic tools to be used in unit testing.
 * 
 * @since 1.0.0
 */
public class TestUtils {
  /**
   * Since sleeps are sometimes necessary, this makes an easy way to ignore InterruptedException's.
   * 
   * @param time time in milliseconds to make the thread to sleep
   */
  public static void sleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      // reset interrupted status
      Thread.currentThread().interrupt();
    }
  }
  
  /**
   * Blocks until the System clock advances at least 1 millisecond.  This will also ensure that 
   * the {@link Clock} class's representation of time has advanced.
   */
  public static void blockTillClockAdvances() {
    new TestCondition() {
      private static final short POLL_INTERVAL_IN_MS = 1;
      
      private final long startTime = Clock.accurateTimeMillis();
      private final long alwaysProgressingStartTime = Clock.accurateForwardProgressingMillis();
      
      @Override
      public boolean get() {
        return Clock.accurateTimeMillis() > startTime && 
                 Clock.accurateForwardProgressingMillis() > alwaysProgressingStartTime;
      }
      
      @Override
      public void blockTillTrue() {
        blockTillTrue(DEFAULT_TIMEOUT, POLL_INTERVAL_IN_MS);
      }
    }.blockTillTrue();
  }
}
