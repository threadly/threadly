package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class FutureListenableFutureTest {
  @Test
  public void cancelBeforeSetTest() {
    FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    testFuture.cancel(true);
    
    assertTrue(testFuture.isCancelled());
    
    TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
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
  
  @Test
  public void addListenerBeforeSetTest() {
    final FutureListenableFuture<Object> testFuture = new FutureListenableFuture<Object>();
    final TestRunnable listener = new TestRunnable();
    
    new Thread(new Runnable() {
      @Override
      public void run() {
        testFuture.addListener(listener);
      }
    }).start();
    
    TestUtils.sleep(50);
    
    final TestFutureImp parentFuture = new TestFutureImp();
    testFuture.setParentFuture(parentFuture);
    
    new TestCondition() {
      @Override
      public boolean get() {
        return ! parentFuture.listeners.isEmpty();
      }
    }.blockTillTrue();
    
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
