package org.threadly.util;

import java.lang.Thread.UncaughtExceptionHandler;

@SuppressWarnings("javadoc")
public class TestUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private int callCount = 0;
  private Thread calledWithThread;
  private Throwable providedThrowable;
  
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    callCount++;
    calledWithThread = t;
    providedThrowable = e;
  }

  public int getCallCount() {
    return callCount;
  }

  public Thread getCalledWithThread() {
    return calledWithThread;
  }

  public Throwable getCalledWithThrowable() {
    return providedThrowable;
  }
}