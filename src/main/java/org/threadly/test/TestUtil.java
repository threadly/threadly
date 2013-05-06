package org.threadly.test;

/**
 * @author jent - Mike Jensen
 */
public class TestUtil {
  public static void sleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      // ignored
    }
  }
}
