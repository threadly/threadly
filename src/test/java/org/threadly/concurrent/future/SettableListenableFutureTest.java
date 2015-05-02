package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
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
  public void cleanup() {
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
    listenersCalledTest(false);
  }
  
  @Test
  public void listenersCalledOnFailureTest() {
    listenersCalledTest(true);
  }
  
  public void listenersCalledTest(boolean failure) {
    TestRunnable tr = new TestRunnable();
    slf.addListener(tr);
    
    if (failure) {
      slf.setFailure(null);
    } else {
      slf.setResult(null);
    }
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    slf.addListener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void addCallbackTest() {
    String result = StringUtils.randomString(5);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.setResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void addCallbackAlreadyDoneFutureTest() {
    String result = StringUtils.randomString(5);
    slf.setResult(result);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
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
  public void addCallbackExecutionExceptionAlreadyDoneTest() {
    Throwable failure = new Exception();
    slf.setFailure(failure);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.addCallback(tfc);
    
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
    assertTrue(slf.cancel(false));
    assertFalse(slf.cancel(false));
    
    assertTrue(slf.isCancelled());
    assertTrue(slf.isDone());
  }
  
  @Test
  public void cancelWithInterruptTest() {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    Thread runningThread = new Thread(btr);
    try {
      runningThread.start();
      slf.setRunningThread(runningThread);
      btr.blockTillStarted();
      
      assertTrue(slf.cancel(true));
      
      // should unblock when interrupted
      btr.blockTillFinished();
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void isDoneTest() {
    assertFalse(slf.isDone());
    
    slf.setResult(null);

    assertTrue(slf.isDone());
  }
  
  @Test
  public void getResultTest() throws InterruptedException, ExecutionException {
    final String testResult = StringUtils.randomString(5);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
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
    final String testResult = StringUtils.randomString(5);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
    try {
      scheduler.prestartAllThreads();
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
  
  @Test
  public void getCancellationTest() throws InterruptedException, ExecutionException {
    slf.cancel(false);
    
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
  }
  
  @Test
  public void getWithTimeoutCancellationTest() throws InterruptedException, ExecutionException, TimeoutException {
    slf.cancel(false);
    
    try {
      slf.get(100, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
  }
}
