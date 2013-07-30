package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerLimiter.FutureFuture;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class PrioritySchedulerLimiterFutureFutureTest {
  @Test
  public void cancelBeforeSetTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    testFuture.cancel(true);
    
    assertTrue(testFuture.isCancelled());
    
    TestFuture parentFuture = new TestFuture();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
  }
  
  @Test
  public void cancelAfterSetTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    TestFuture parentFuture = new TestFuture();
    testFuture.setParentFuture(parentFuture);
    
    testFuture.cancel(true);
    
    assertTrue(parentFuture.canceled);
    assertTrue(testFuture.isCancelled());
  }
  
  @Test
  public void isDoneTest() {
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    assertFalse(testFuture.isDone());
    
    TestFuture parentFuture = new TestFuture();
    testFuture.setParentFuture(parentFuture);
    
    assertTrue(testFuture.isDone());
  }
  
  @Test
  public void addListenerBeforeSetTest() {
    final FutureFuture<Object> testFuture = new FutureFuture<Object>();
    final TestRunnable listener = new TestRunnable();
    
    new Thread(new Runnable() {
      @Override
      public void run() {
        testFuture.addListener(listener);
      }
    }).start();
    
    TestUtils.sleep(50);
    
    final TestFuture parentFuture = new TestFuture();
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
    FutureFuture<Object> testFuture = new FutureFuture<Object>();
    TestFuture parentFuture = new TestFuture();
    testFuture.setParentFuture(parentFuture);
    
    TestRunnable listener = new TestRunnable();
    
    testFuture.addListener(listener);
    
    assertEquals(parentFuture.listeners.size(), 1);
    assertTrue(parentFuture.listeners.get(0) == listener);
  }
  
  private class TestFuture implements ListenableFuture<Object> {
    public final Object result = new Object();
    private boolean canceled = false;
    private List<Runnable> listeners = new LinkedList<Runnable>();
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      canceled = true;
      
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
      return result;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return result;
    }

    @Override
    public void addListener(Runnable listener) {
      listeners.add(listener);
    }

    @Override
    public void addListener(Runnable listener, Executor executor) {
      listeners.add(listener);
    }
  }
}
