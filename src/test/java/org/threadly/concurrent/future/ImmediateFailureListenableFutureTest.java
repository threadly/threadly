package org.threadly.concurrent.future;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ImmediateFailureListenableFutureTest {
  @Test
  public void immediateFailureFutureTest() {
    Exception failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(failure);
    
    assertTrue(testFuture.isDone());
    try {
      testFuture.get();
      fail("Exception should have thrown");
    } catch (InterruptedException e) {
      fail("ExecutionException should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
    try {
      testFuture.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (InterruptedException e) {
      fail("ExecutionException should have thrown");
    } catch (TimeoutException e) {
      fail("ExecutionException should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == failure);
    }
  }
  
  @Test
  public void immediateFailureFutureCancelTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(null);
    
    assertFalse(testFuture.cancel(true));
    assertFalse(testFuture.isCancelled());
  }
  
  @Test
  public void immediateFailureFutureAddListenerTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(null);
    
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
  public void immediateFailureFutureAddCallbackTest() throws InterruptedException, ExecutionException {
    Throwable failure = new Exception();
    ListenableFuture<?> testFuture = new ImmediateFailureListenableFuture<Object>(failure);
    
    TestFutureCallback tfc = new TestFutureCallback();
    testFuture.addCallback(tfc);
    assertTrue(tfc.getLastFailure() == failure);
    
    tfc = new TestFutureCallback();
    testFuture.addCallback(tfc, null);
    assertTrue(tfc.getLastFailure() == failure);
    
    tfc = new TestFutureCallback();
    testFuture.addCallback(tfc, new SameThreadSubmitterExecutor());
    assertTrue(tfc.getLastFailure() == failure);
  }
}
