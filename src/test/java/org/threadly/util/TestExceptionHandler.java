package org.threadly.util;

import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("javadoc")
public class TestExceptionHandler implements ExceptionHandlerInterface {
  private final AtomicInteger callCount = new AtomicInteger(0);
  private Throwable lastProvidedThrowable = null;
  
  @Override
  public void handleException(Throwable thrown) {
    callCount.incrementAndGet();
    lastProvidedThrowable = thrown;
  }
  
  public int getCallCount() {
    return callCount.get();
  }
  
  public Throwable getLastThrowable() {
    return lastProvidedThrowable;
  }
}