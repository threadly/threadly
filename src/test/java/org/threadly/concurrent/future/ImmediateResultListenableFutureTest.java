package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ImmediateResultListenableFutureTest {
  @Test
  public void nullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);
    
    ImmediateListenableFutureTest.resultTest(testFuture, null);
  }
  
  @Test
  public void nonNullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    ImmediateListenableFutureTest.resultTest(testFuture, result);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void addListenerTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void addCallbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    ImmediateListenableFutureTest.resultAddCallbackTest(testFuture, result);
  }
}
