package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class FutureFutureTest {
  @Test
  public void setParentFutureFail() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    try {
      testFuture.setParentFuture(null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      testFuture.setParentFuture(new TestFutureImp());
      testFuture.setParentFuture(new TestFutureImp());  // should not allow two sets
      fail("Exception should have thrown");
    } catch (IllegalStateException e) {
      // expected
    }
  }
  
  @Test
  public void cancelBeforeSetTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    testFuture.cancel(true);
    
    assertFalse(testFuture.isCancelled());  // unable to cancel without parent being set
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertFalse(parentFuture.canceled);
    assertFalse(testFuture.isCancelled());
  }
  
  @Test
  public void cancelAfterSetTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    testFuture.cancel(true);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
  }
  
  @Test
  public void isDoneTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    assertFalse(testFuture.isDone());
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(testFuture.isDone());
  }
}
