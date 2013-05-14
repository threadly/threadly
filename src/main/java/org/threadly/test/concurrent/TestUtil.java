package org.threadly.test.concurrent;

/**
 * Generic tools to be used in unit testing.
 * 
 * @author jent - Mike Jensen
 */
public class TestUtil {
  /**
   * Since sleeps are sometimes necessary, this makes
   * an easy way to ignore InterruptedException's.
   * 
   * @param time time in milliseconds to make the thread to sleep
   */
  public static void sleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      // ignored
    }
  }
  
  /**
   * Blocks until the System clock advances at least 1 millisecond.
   */
  public static void blockTillClockAdvances() {
    new TestCondition() {
      private static final int TIMEOUT_IN_MS = 100;
      private static final int POLL_INTERVAL_IN_MS = 1;
      
      private final long startTime = System.currentTimeMillis();
      
      @Override
      public boolean get() {
        return System.currentTimeMillis() != startTime;
      }
      
      @Override
      public void blockTillTrue() {
        blockTillTrue(TIMEOUT_IN_MS, POLL_INTERVAL_IN_MS);
      }
    }.blockTillTrue();
  }
}
