package org.threadly.test;

/**
 * Tools to be used in unit testing
 * 
 * @author jent - Mike Jensen
 */
public class TestUtil {
  /**
   * Since sleeps are sometimes necessary, this makes
   * an easy way to ignore InterruptedException's,
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
}
