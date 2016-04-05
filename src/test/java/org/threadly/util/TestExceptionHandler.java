package org.threadly.util;

import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("javadoc")
public class TestExceptionHandler implements ExceptionHandler {
  private Thread calledWithThread = null;
  private final AtomicInteger callCount = new AtomicInteger(0);
  private Throwable lastProvidedThrowable = null;
  
  @Override
  public void handleException(Throwable thrown) {
    calledWithThread = Thread.currentThread();
    callCount.incrementAndGet();
    lastProvidedThrowable = thrown;
  }
  
  public int getCallCount() {
    return callCount.get();
  }

  public Thread getCalledWithThread() {
    return calledWithThread;
  }
  
  public Throwable getLastThrowable() {
    return lastProvidedThrowable;
  }
}