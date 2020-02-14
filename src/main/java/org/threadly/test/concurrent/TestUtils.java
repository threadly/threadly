package org.threadly.test.concurrent;

import org.threadly.util.Clock;

/**
 * Generic tools to be used in unit testing.
 * 
 * @deprecated Now provided by org.threadly:threadly-test:0,1 artifact
 * 
 * @since 1.0.0
 */
@Deprecated
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
    long startTime = Clock.accurateTimeMillis();
    long alwaysProgressingStartTime = Clock.accurateForwardProgressingMillis();
    new TestCondition(() -> Clock.accurateTimeMillis() > startTime && 
                              Clock.accurateForwardProgressingMillis() > alwaysProgressingStartTime)
        .blockTillTrue(TestCondition.DEFAULT_TIMEOUT, 1);
  }
}
