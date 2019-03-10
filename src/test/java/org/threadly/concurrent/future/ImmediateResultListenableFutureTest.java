package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SameThreadSubmitterExecutor;

@SuppressWarnings("javadoc")
public class ImmediateResultListenableFutureTest extends ThreadlyTester {
  @Test
  public void nullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ImmediateListenableFutureTest.resultTest(new ImmediateResultListenableFuture<>(null), null);
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
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(result);
    
    ImmediateListenableFutureTest.resultTest(testFuture, result);
  }
  
  @Test
  public void cancelTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);

    ImmediateListenableFutureTest.cancelTest(testFuture);
  }
  
  @Test
  public void listenerTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    ImmediateListenableFutureTest.listenerTest(testFuture);
  }
  
  @Test
  public void callbackTest() {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(result);
    
    ImmediateListenableFutureTest.resultCallbackTest(testFuture, result);
  }
  
  @Test
  public void mapFailureTest() {
    // should be straight through
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }));
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }, 
                                                   SameThreadSubmitterExecutor.instance()));
    assertTrue(testFuture == testFuture.mapFailure(Exception.class, 
                                                   (t) -> { throw new RuntimeException(); }, 
                                                   SameThreadSubmitterExecutor.instance(), null));
  }
  
  @Test
  public void flatMapFailureTest() {
    // should be straight through
    ListenableFuture<Object> testFuture = new ImmediateResultListenableFuture<>(null);
    
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException())));
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException()), 
                                                       SameThreadSubmitterExecutor.instance()));
    assertTrue(testFuture == testFuture.flatMapFailure(Exception.class, 
                                                       (t) -> FutureUtils.immediateFailureFuture(new RuntimeException()), 
                                                       SameThreadSubmitterExecutor.instance(), null));
  }
  
  @Test
  public void getRunningStackTraceTest() {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<>(null);
    
    ImmediateListenableFutureTest.getRunningStackTraceTest(testFuture);
  }
}
