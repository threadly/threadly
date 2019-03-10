package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ImmediateListenableFutureTest extends ThreadlyTester {
  public static void cancelTest(ListenableFuture<?> testFuture) {
    assertFalse(testFuture.cancel(true));
    assertFalse(testFuture.isCancelled());
  }
  
  public static void listenerTest(ListenableFuture<?> testFuture) {
    TestRunnable tr = new TestRunnable();
    testFuture.listener(tr);
    assertTrue(tr.ranOnce());
    
    tr = new TestRunnable();
    testFuture.listener(tr, null);
    assertTrue(tr.ranOnce());
    
    tr = new TestRunnable();
    testFuture.listener(tr, new SameThreadSubmitterExecutor());
    assertTrue(tr.ranOnce());
  }
  
  public static void resultTest(ListenableFuture<?> testFuture, Object expectedResult) throws InterruptedException, 
                                                                                              ExecutionException, 
                                                                                              TimeoutException {
    assertTrue(testFuture.isDone());
    assertTrue(testFuture.get() == expectedResult);
    assertTrue(testFuture.get(1, TimeUnit.MILLISECONDS) == expectedResult);
  }
  
  public static void resultCallbackTest(ListenableFuture<?> testFuture, Object expectedResult) {
    TestFutureCallback tfc = new TestFutureCallback();
    testFuture.callback(tfc);
    assertTrue(tfc.getLastResult() == expectedResult);
    
    tfc = new TestFutureCallback();
    testFuture.callback(tfc, null);
    assertTrue(tfc.getLastResult() == expectedResult);
    
    tfc = new TestFutureCallback();
    testFuture.callback(tfc, new SameThreadSubmitterExecutor());
    assertTrue(tfc.getLastResult() == expectedResult);
  }
  
  public static void failureTest(ListenableFuture<?> testFuture, Throwable expectedFailure) {
    assertTrue(testFuture.isDone());
    try {
      testFuture.get();
      fail("Exception should have thrown");
    } catch (InterruptedException e) {
      fail("ExecutionException should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == expectedFailure);
    }
    try {
      testFuture.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (InterruptedException e) {
      fail("ExecutionException should have thrown");
    } catch (TimeoutException e) {
      fail("ExecutionException should have thrown");
    } catch (ExecutionException e) {
      assertTrue(e.getCause() == expectedFailure);
    }
  }
  
  public static void failureCallbackTest(ListenableFuture<?> testFuture, Throwable expectedFailure) {
    TestFutureCallback tfc = new TestFutureCallback();
    testFuture.callback(tfc);
    assertTrue(tfc.getLastFailure() == expectedFailure);
    
    tfc = new TestFutureCallback();
    testFuture.callback(tfc, null);
    assertTrue(tfc.getLastFailure() == expectedFailure);
    
    tfc = new TestFutureCallback();
    testFuture.callback(tfc, new SameThreadSubmitterExecutor());
    assertTrue(tfc.getLastFailure() == expectedFailure);
  }
  
  public static void getRunningStackTraceTest(ListenableFuture<?> testFuture) {
    assertNull(testFuture.getRunningStackTrace());
  }
}
