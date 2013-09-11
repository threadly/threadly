package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class FutureListenableFutureTest {
  @Test
  public void setParentFutureFail() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    try {
      testFuture.setParentFuture(null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      // FutureListenableFuture can not accept normale futures
      testFuture.setParentFuture(new FutureTask<Object>(new TestRunnable(), null));
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      testFuture.setParentFuture((Future<?>)new TestFutureImp());
      testFuture.setParentFuture((Future<?>)new TestFutureImp());  // should not allow two sets
      fail("Exception should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }
  
  @Test
  public void cancelBeforeSetTest() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    testFuture.cancel(true);
    
    assertFalse(testFuture.isCancelled());  // unable to cancel without parent being set
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertFalse(parentFuture.canceled);
    assertFalse(testFuture.isCancelled());
  }
  
  @Test
  public void cancelAfterSetTest() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    testFuture.cancel(true);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
  }
  
  @Test
  public void isDoneTest() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    assertFalse(testFuture.isDone());
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(testFuture.isDone());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addListenerFail() {
    FutureListenableFuture<Object> future = new FutureListenableFuture<Object>();
    
    future.addListener(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void addListenerBeforeSetTest() {
    final FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    final TestRunnable listener = new TestRunnable();
    
    testFuture.addListener(listener);
    
    final TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertEquals(parentFuture.listeners.size(), 1);
    assertTrue(parentFuture.listeners.get(0) == listener);
  }
  
  @Test
  public void addListenerAfterSetTest() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    TestRunnable listener = new TestRunnable();
    
    testFuture.addListener(listener);
    
    assertEquals(parentFuture.listeners.size(), 1);
    assertTrue(parentFuture.listeners.get(0) == listener);
  }
}
