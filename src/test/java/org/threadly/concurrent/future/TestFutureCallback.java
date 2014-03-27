package org.threadly.concurrent.future;

import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("javadoc")
public class TestFutureCallback implements FutureCallback<Object> {
  private final AtomicInteger callCount = new AtomicInteger(0);
  private Object lastResult = null;
  private Throwable lastFailure = null;
  
  public int getCallCount() {
    return callCount.get();
  }
  
  public Object getLastResult() {
    return lastResult;
  }
  
  public Throwable getLastFailure() {
    return lastFailure;
  }

  @Override
  public void handleResult(Object result) {
    callCount.incrementAndGet();
    
    lastResult = result;
  }

  @Override
  public void handleFailure(Throwable t) {
    callCount.incrementAndGet();
    
    lastFailure = t;
  }
}
