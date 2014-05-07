package org.threadly.concurrent.future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ImmediateResultListenableFutureTest {
  @Test
  public void immediateResultFutureNullResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);
    
    assertTrue(testFuture.isDone());
    assertNull(testFuture.get());
    assertNull(testFuture.get(1, TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void immediateResultFutureTest() throws InterruptedException, ExecutionException, TimeoutException {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    assertTrue(testFuture.isDone());
    assertTrue(testFuture.get() == result);
    assertTrue(testFuture.get(1, TimeUnit.MILLISECONDS) == result);
  }
  
  @Test
  public void immediateResultFutureCancelTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);
    
    assertFalse(testFuture.cancel(true));
    assertFalse(testFuture.isCancelled());
  }
  
  @Test
  public void immediateResultFutureAddListenerTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(null);
    
    TestRunnable tr = new TestRunnable();
    testFuture.addListener(tr);
    assertTrue(tr.ranOnce());
    
    tr = new TestRunnable();
    testFuture.addListener(tr, null);
    assertTrue(tr.ranOnce());
    
    tr = new TestRunnable();
    testFuture.addListener(tr, new SameThreadSubmitterExecutor());
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void immediateResultFutureAddCallbackTest() throws InterruptedException, ExecutionException {
    Object result = new Object();
    ListenableFuture<?> testFuture = new ImmediateResultListenableFuture<Object>(result);
    
    TestFutureCallback tfc = new TestFutureCallback();
    testFuture.addCallback(tfc);
    assertTrue(tfc.getLastResult() == result);
    
    tfc = new TestFutureCallback();
    testFuture.addCallback(tfc, null);
    assertTrue(tfc.getLastResult() == result);
    
    tfc = new TestFutureCallback();
    testFuture.addCallback(tfc, new SameThreadSubmitterExecutor());
    assertTrue(tfc.getLastResult() == result);
  }
}
