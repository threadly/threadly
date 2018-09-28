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
  public void addListenerTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void addCallbackTest() {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(failure);
    
    ImmediateListenableFutureTest.failureAddCallbackTest(testFuture, failure);
  }
  
  @Test
  public void getRunningStackTraceTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<>(null);
    
    ImmediateListenableFutureTest.getRunningStackTraceTest(testFuture);
  }
}
