package org.threadly.concurrent.future;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class ImmediateFailureListenableFutureTest extends ThreadlyTester {
  @Test
  public void getTest() {
    Exception failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(failure);
    
    ImmediateListenableFutureTest.failureTest(testFuture, failure);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void listenerTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(null);
    
    ImmediateListenableFutureTest.listenerTest(testFuture);
  }
  
  @Test
  public void callbackTest() {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(failure);
    
    ImmediateListenableFutureTest.failureCallbackTest(testFuture, failure);
  }
  
  @Test
  public void getRunningStackTraceTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(null);
    
    ImmediateListenableFutureTest.getRunningStackTraceTest(testFuture);
  }
}
