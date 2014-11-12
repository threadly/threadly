package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class SettableListenableFutureTest {
  protected SettableListenableFuture<String> slf;
  
  @Before
  public void setup() {
    slf = new SettableListenableFuture<String>();
  }
  
  @After
  public void tearDown() {
    slf = null;
  }
  
  @Test (expected = IllegalStateException.class)
  public void setResultResultFail() {
    slf.setResult(null);
    slf.setResult(null);
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void setFailureResultFail() {
    slf.setFailure(null);
    slf.setResult(null);
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void setResultFailureFail() {
    slf.setResult(null);
    slf.setFailure(null);
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void setFailureFailureFail() {
    slf.setFailure(null);
    slf.setFailure(null);
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearResultFail() {
    slf.clearResult();
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void getAfterClearResultFail() throws InterruptedException, ExecutionException {
    slf.setResult(null);
    slf.clearResult();
    slf.get();
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void getAfterClearFailureResultFail() throws InterruptedException, ExecutionException {
    slf.setFailure(null);
    slf.clearResult();
    slf.get();
    fail("Should have thrown exception");
  }
  
  @Test
  public void listenersCalledOnResultTest() {
    TestRunnable tr = new TestRunnable();
    slf.addListener(tr);
    
    slf.setResult(null);
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    slf.addListener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void listenersCalledOnFailureTest() {
    TestRunnable tr = new TestRunnable();
    slf.addListener(tr);
    
    slf.setFailure(null);
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    slf.addListener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void addCallbackTest() {
    String result = "addCallbackTest";
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackAlreadyDoneFutureTest() {
    String result = "addCallbackAlreadyDoneFutureTest";
    slf.setResult(result);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackExecutionExceptionAlreadyDoneTest() {
    Throwable failure = new Exception();
    slf.setFailure(failure);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addCallbackExecutionExceptionTest() {
    Throwable failure = new Exception();
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setFailure(failure);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void addAsCallbackResultTest() throws InterruptedException, ExecutionException {
    String testResult = StringUtils.randomString(5);
    ListenableFuture<String> resultFuture = new ImmediateResultListenableFuture<String>(testResult);
    
    resultFuture.addCallback(slf);
    
    assertTrue(slf.isDone());
    assertEquals(testResult, slf.get());
  }
  
  @Test
  public void addAsCallbackFailureTest() throws InterruptedException {
    Exception e = new Exception();
    ListenableFuture<String> failureFuture = new ImmediateFailureListenableFuture<String>(e);
    
    failureFuture.addCallback(slf);
    
    assertTrue(slf.isDone());
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() == e);
    }
  }
  
  @Test
  public void cancelTest() {
    assertFalse(slf.cancel(false));
    assertFalse(slf.cancel(true));
    assertFalse(slf.isCancelled());
    assertFalse(slf.isDone());
  }
  
  @Test
  public void isDoneTest() {
    assertFalse(slf.isDone());
    
    slf.setResult(null);

    assertTrue(slf.isDone());
  }
  
  @Test
  public void getResultTest() throws InterruptedException, ExecutionException {
    final String testResult = "getResultTest";
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1, 1, 100);
    try {
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          slf.setResult(testResult);
        }
      }, DELAY_TIME);
      
      assertTrue(slf.get() == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithTimeoutResultTest() throws InterruptedException, 
                                                ExecutionException, 
                                                TimeoutException {
    final String testResult = "getWithTimeoutResultTest";
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1, 1, 100);
    try {
      scheduler.prestartAllCoreThreads();
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          slf.setResult(testResult);
        }
      }, DELAY_TIME);
      
      assertTrue(slf.get(DELAY_TIME * 10, TimeUnit.MILLISECONDS) == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getTimeoutTest() throws InterruptedException, 
                                      ExecutionException {
    long startTime = Clock.accurateForwardProgressingMillis();
    try {
      slf.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (TimeoutException e) {
      // expected
    }
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test (expected = ExecutionException.class)
  public void getNullExceptionTest() throws InterruptedException, 
                                            ExecutionException {
    slf.setFailure(null);
    slf.get();
  }
  
  @Test
  public void getExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    slf.setFailure(failure);
    
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, 
                                                            TimeoutException {
    Exception failure = new Exception();
    slf.setFailure(failure);
    
    try {
      slf.get(100, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
}
