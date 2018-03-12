package org.threadly.concurrent.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ImmediateResultListenableFutureTest {
  @Test
  public void nullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(new ImmediateResultListenableFuture<Void>(null), null);
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.NULL_RESULT, null);
  }
  
  @Test
  public void booleanResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.BOOLEAN_FALSE_RESULT, false);
    ImmediateListenableFutureTest.resultTest(ImmediateResultListenableFuture.BOOLEAN_TRUE_RESULT, true);
  }
  
  @Test
  public void nonNullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    ImmediateListenableFutureTest.resultTest(testFuture, result);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Void>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void addListenerTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Void>(null);
    
    ImmediateListenableFutureTest.addListenerTest(testFuture);
  }
  
  @Test
  public void addCallbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    ImmediateListenableFutureTest.resultAddCallbackTest(testFuture, result);
  }
}
