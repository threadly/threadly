package org.threadly;

/**
 * Class which contains constants which impact how long the unit tests run.
 * 
 * This way if we want to stress test the code, we only have to adjust the 
 * constants from one class.  In all cases, lower numbers mean faster tests.
 */
public class TestConstants {
  /**
   * A profile for the amount of load and verification to be done.
   */
  public enum TestLoad { 
    /**
     * Fastest, minimal testing.
     */
    Speedy, 
    /**
     * Good general place to start, still very fast.
     */
    Normal, 
    /**
     * Very slow, but gives a good extensive run.
     */
    Stress
  };
  
  /**
   * Can easily adjust all constants in this file by changing the load 
   * profile.
   */
  public static final TestLoad TEST_PROFILE = TestLoad.Normal;
  
  /**
   * Represents the number of iterations, or possibly runnables 
   * to be verified in a given test.
   * 
   * Should be at least 2, recommended at least 5, but can go very high.
   */
  public static final int TEST_QTY;
  /**
   * Represents a delay for scheduling or recurring tasks.
   * 
   * Should be at least 10 or 20, anything higher than 50 is basically wasted for most systems.
   */
  public static final int SCHEDULE_DELAY;
  /**
   * Represents how many times we want to verify a given action.  For example 
   * for a recurring task, how many times should it run.
   * 
   * Should be at least 2, but can go very high.
   */
  public static final int CYCLE_COUNT;
  /**
   * Represents a delay where we have to block to verify a block happened correctly.
   * 
   * Should be at least 10 or 20, anything higher than 100 is basically wasted for most systems.
   */
  public static final int DELAY_TIME;
  
  static {
    switch (TEST_PROFILE) {
      case Speedy:
        TEST_QTY = 2;
        SCHEDULE_DELAY = 10;
        CYCLE_COUNT = 2;
        DELAY_TIME = 10;
        break;
      case Normal:
        TEST_QTY = 5;
        SCHEDULE_DELAY = 20;
        CYCLE_COUNT = 2;
        DELAY_TIME = 10;
        break;
      case Stress:
        TEST_QTY = 100;
        SCHEDULE_DELAY = 50;
        CYCLE_COUNT = 10;
        DELAY_TIME = 50;
        break;
      default:
        throw new UnsupportedOperationException("Load not supported: " + TEST_PROFILE);
    }
  }
}
