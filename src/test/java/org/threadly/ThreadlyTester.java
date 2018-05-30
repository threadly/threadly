package org.threadly;

import java.lang.Thread.UncaughtExceptionHandler;

import org.junit.Rule;
import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;

/**
 * Class which contains constants which impact how long the unit tests run, as well as anything 
 * commonly needed across tests.
 */
@SuppressWarnings("javadoc")
public abstract class ThreadlyTester {
  /**
   * Profile to use when no specific profile is specified. 
   */
  private static final TestLoad DEFAULT_PROFILE = TestLoad.Normal;
  
  /**
   * Indicates to unit tests if they should allow extra time allowances for actions to complete.
   */
  public static final boolean SLOW_MACHINE;
  
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
  }
  
  /**
   * Can easily adjust all constants in this file by changing the load 
   * profile.
   */
  public static final TestLoad TEST_PROFILE;
  
  /**
   * Represents the number of iterations, or possibly runnables 
   * to be verified in a given test.
   * 
   * Should be at least 2, recommended at least 5, but can go very high.
   */
  public static final int TEST_QTY;
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
   * Should be at least 10 to 20, anything higher than 100 is basically wasted for most systems.
   */
  public static final int DELAY_TIME;
  /**
   * Allowed variance for a specific OS's clock inaccuracy
   */
  public static final int ALLOWED_VARIANCE;
  
  static {
    SLOW_MACHINE = StringUtils.nullToEmpty(System.getProperty("systemSpeed")).equalsIgnoreCase("slow");
    System.out.println("Running tests with extra allowed delay: " + SLOW_MACHINE);
    
    if(System.getProperty("os.name").toLowerCase().contains("win")) {
      ALLOWED_VARIANCE = 2;
    } else {
      ALLOWED_VARIANCE = 0;
    }
    
    String testProfileStr = System.getProperty("testProfile");
    TestLoad testProfile = null;
    if (testProfileStr != null) {
      for (TestLoad tl : TestLoad.values()) {
        if (tl.name().equalsIgnoreCase(testProfileStr)) {
          testProfile = tl;
          break;
        }
      }
    }
    if (testProfile == null) {
      TEST_PROFILE = DEFAULT_PROFILE;
    } else {
      TEST_PROFILE = testProfile;
    }
    System.out.println("Running tests with profile: " + TEST_PROFILE);
    
    switch (TEST_PROFILE) {
      case Speedy:
        TEST_QTY = 2;
        CYCLE_COUNT = 2;
        DELAY_TIME = 10;
        break;
      case Normal:
        TEST_QTY = 5;
        CYCLE_COUNT = 10;
        DELAY_TIME = 10;
        break;
      case Stress:
        TEST_QTY = 100;
        CYCLE_COUNT = 20;
        DELAY_TIME = 20;
        break;
      default:
        throw new UnsupportedOperationException("Load not supported: " + TEST_PROFILE);
    }
  }

  public static void setIgnoreExceptionHandler() {
    IgnoreExceptionHandler ieh = new IgnoreExceptionHandler();
    
    Thread.currentThread().setUncaughtExceptionHandler(null);
    Thread.setDefaultUncaughtExceptionHandler(ieh);
    
    ExceptionUtils.setThreadExceptionHandler(null);
    ExceptionUtils.setInheritableExceptionHandler(null);
    ExceptionUtils.setDefaultExceptionHandler(ieh);
  }
  
  @Rule
  public RepeatRule repeatRule = new RepeatRule();
  
  private static class IgnoreExceptionHandler implements UncaughtExceptionHandler, 
                                                         ExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      // ignored
    }

    @Override
    public void handleException(Throwable thrown) {
      // ignored
    }
  }
}
