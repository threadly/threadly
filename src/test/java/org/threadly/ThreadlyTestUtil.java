package org.threadly;

import java.lang.Thread.UncaughtExceptionHandler;

import org.threadly.util.ExceptionHandler;
import org.threadly.util.ExceptionUtils;

@SuppressWarnings("javadoc")
public class ThreadlyTestUtil {
  public static void setIgnoreExceptionHandler() {
    IgnoreExceptionHandler ieh = new IgnoreExceptionHandler();
    
    Thread.currentThread().setUncaughtExceptionHandler(null);
    Thread.setDefaultUncaughtExceptionHandler(ieh);
    
    ExceptionUtils.setThreadExceptionHandler(null);
    ExceptionUtils.setInheritableExceptionHandler(null);
    ExceptionUtils.setDefaultExceptionHandler(ieh);
  }
  
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
