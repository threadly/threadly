package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.StrictPriorityScheduledExecutor;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ListenableFutureResultTest {
  private ListenableFutureResult<String> lfr;
  
  @Before
  public void setup() {
    lfr = new ListenableFutureResult<String>();
  }
  
  @After
  public void tearDown() {
    lfr = null;
  }
  
  @Test (expected = IllegalStateException.class)
  public void setResultResultFail() {
    lfr.setResult(null);
    lfr.setResult(null);
  }
  
  @Test (expected = IllegalStateException.class)
  public void setFailureResultFail() {
    lfr.setFailure(null);
    lfr.setResult(null);
  }
  
  @Test (expected = IllegalStateException.class)
  public void setResultFailureFail() {
    lfr.setResult(null);
    lfr.setFailure(null);
  }
  
  @Test (expected = IllegalStateException.class)
  public void setFailureFailureFail() {
    lfr.setFailure(null);
    lfr.setFailure(null);
  }
  
  @Test
  public void listenersCalledOnResultTest() {
    TestRunnable tr = new TestRunnable();
    lfr.addListener(tr);
    
    lfr.setResult(null);
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    lfr.addListener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void listenersCalledOnFailureTest() {
    TestRunnable tr = new TestRunnable();
    lfr.addListener(tr);
    
    lfr.setFailure(null);
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    lfr.addListener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void cancelTest() {
    assertFalse(lfr.cancel(false));
    assertFalse(lfr.cancel(true));
    assertFalse(lfr.isCancelled());
    assertFalse(lfr.isDone());
  }
  
  @Test
  public void isDoneTest() {
    assertFalse(lfr.isDone());
    
    lfr.setResult(null);

    assertTrue(lfr.isDone());
  }
  
  @Test
  public void getResultTest() throws InterruptedException, ExecutionException {
    final String testResult = "getResultTest";
    
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          lfr.setResult(testResult);
        }
      }, SCHEDULE_DELAY);
      
      assertTrue(lfr.get() == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithTimeoutResultTest() throws InterruptedException, 
                                                ExecutionException, 
                                                TimeoutException {
    final String testResult = "getWithTimeoutResultTest";
    
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      scheduler.schedule(new Runnable() {
        @Override
        public void run() {
          lfr.setResult(testResult);
        }
      }, SCHEDULE_DELAY);
      
      assertTrue(lfr.get(SCHEDULE_DELAY * 4, TimeUnit.MILLISECONDS) == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getTimeoutTest() throws InterruptedException, 
                                      ExecutionException {
    long startTime = System.currentTimeMillis();
    try {
      lfr.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (TimeoutException e) {
      // expected
    }
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test (expected = ExecutionException.class)
  public void getNullExceptionTest() throws InterruptedException, 
                                            ExecutionException {
    lfr.setFailure(null);
    lfr.get();
  }
  
  @Test
  public void getExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    lfr.setFailure(failure);
    
    try {
      lfr.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, 
                                                            TimeoutException {
    Exception failure = new Exception();
    lfr.setFailure(failure);
    
    try {
      lfr.get(100, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
}
