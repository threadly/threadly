package org.threadly;

import java.lang.Thread.UncaughtExceptionHandler;

@SuppressWarnings("javadoc")
public class ThreadlyTestUtil {
  public static void setDefaultUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        // ignored
      }
    });
  }
}
