package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class FutureFutureTest {
  @Test
  public void cancelBeforeSetTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    testFuture.cancel(true);
    
    assertTrue(testFuture.isCancelled());
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
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
