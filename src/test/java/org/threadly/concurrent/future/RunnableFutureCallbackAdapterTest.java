package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings({"javadoc", "deprecation"})
public class RunnableFutureCallbackAdapterTest {
  @Test
  public void constructorTest() {
    Future<Object> future = new TestFutureImp(false);
    FutureCallback<Object> callback = new TestFutureCallback();
    RunnableFutureCallbackAdapter<?> rfca = new RunnableFutureCallbackAdapter<>(future, callback);
    
    assertTrue(rfca.future == future);
    assertTrue(rfca.callback == callback);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new RunnableFutureCallbackAdapter<>(null, new TestFutureCallback());
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new RunnableFutureCallbackAdapter<>(new TestFutureImp(false), null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void resultTest() {
    SettableListenableFuture<Object> future = new SettableListenableFuture<>();
    TestFutureCallback callback = new TestFutureCallback();
    Object result = new Object();
    future.setResult(result);
    
    RunnableFutureCallbackAdapter<?> rfca = new RunnableFutureCallbackAdapter<>(future, callback);
    rfca.run();
    
    assertTrue(callback.getLastResult() == result);
    assertNull(callback.getLastFailure());
  }
  
  @Test
  public void executionExceptionTest() {
    SettableListenableFuture<Object> future = new SettableListenableFuture<>();
    TestFutureCallback callback = new TestFutureCallback();
    Exception failure = new Exception();
    future.setFailure(failure);
    
    RunnableFutureCallbackAdapter<?> rfca = new RunnableFutureCallbackAdapter<>(future, callback);
    rfca.run();
    
    assertTrue(callback.getLastFailure() == failure);
    assertNull(callback.getLastResult());
  }
  
  @Test
  public void cancellationExceptionTest() {
    Future<Object> future = new TestFutureImp(false);
    TestFutureCallback callback = new TestFutureCallback();
    future.cancel(true);
    
    RunnableFutureCallbackAdapter<?> rfca = new RunnableFutureCallbackAdapter<>(future, callback);
    rfca.run();
    
    assertTrue(callback.getLastFailure() instanceof CancellationException);
    assertNull(callback.getLastResult());
  }
  
  @Test
  public void interruptedExceptionTest() throws InterruptedException, TimeoutException {
    SettableListenableFuture<Object> future = new SettableListenableFuture<>();
    TestFutureCallback callback = new TestFutureCallback();
    final AsyncVerifier av = new AsyncVerifier();
    
    final RunnableFutureCallbackAdapter<?> rfca = new RunnableFutureCallbackAdapter<>(future, callback);
    final Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          rfca.run();
          av.fail("Exception did not throw");
        } catch (RuntimeException expected) {
          av.signalComplete();
        }
      }
    });
    
    t.start();
    new TestCondition(() -> t.isAlive()).blockTillTrue();
    
    t.interrupt();
    
    av.waitForTest();
    assertTrue(callback.getLastFailure() instanceof InterruptedException);
    assertNull(callback.getLastResult());
  }
}
