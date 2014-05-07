package org.threadly.concurrent.future;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ImmediateFailureListenableFutureTest {
  @Test
  public void getTest() {
    Exception failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(failure);
    
    ImmediateListenableFutureTest.failureTest(testFuture, failure);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void addListenerTest() {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void addCallbackTest() {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(failure);
    
    ImmediateListenableFutureTest.failureAddCallbackTest(testFuture, failure);
  }
}
