package org.threadly.test;

public class TestUtil {
  public static void sleep(long time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      // ignored
    }
  }
}
