package org.threadly.concurrent.wrapper.compatibility;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.TestDelayed;
import org.threadly.concurrent.future.TestFutureCallback;
import org.threadly.concurrent.future.TestFutureImp;
import org.threadly.concurrent.wrapper.compatibility.AbstractExecutorServiceWrapper.ScheduledFutureDelegate;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public class ScheduledFutureDelegateTest extends ThreadlyTester {
  @Test
  public void getDelayTest() {
    int startVal = TEST_QTY * -1;
    int endVal = TEST_QTY;
    for(int i = startVal; i <= endVal; i++) {
      TestDelayed td = new TestDelayed(i);
      ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(null, td);
      
      assertEquals(td.getDelay(TimeUnit.MILLISECONDS), testItem.getDelay(TimeUnit.MILLISECONDS));
      assertEquals(td.getDelay(TimeUnit.NANOSECONDS), testItem.getDelay(TimeUnit.NANOSECONDS));
      assertEquals(td.getDelay(TimeUnit.SECONDS), testItem.getDelay(TimeUnit.SECONDS));

      TestDelayed tdFail = new TestDelayed(i + 10000);
      assertTrue(testItem.getDelay(TimeUnit.MILLISECONDS) != tdFail.getDelay(TimeUnit.MILLISECONDS));
      assertTrue(testItem.getDelay(TimeUnit.NANOSECONDS) != tdFail.getDelay(TimeUnit.NANOSECONDS));
      assertTrue(testItem.getDelay(TimeUnit.SECONDS) != tdFail.getDelay(TimeUnit.SECONDS));
    }
  }
  
  @Test
  public void compareToTest() {
    int startVal = TEST_QTY * -1;
    int endVal = TEST_QTY;
    for(int i = startVal; i <= endVal; i++) {
      TestDelayed td = new TestDelayed(i);
      ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(null, td);
      
      assertEquals(0, testItem.compareTo(td));
      assertEquals(0, testItem.compareTo(new TestDelayed(i)));

      TestDelayed tdGreater = new TestDelayed(i + 1000);
      assertTrue(testItem.compareTo(tdGreater) < 0);
      
      TestDelayed tdLesser = new TestDelayed(Short.MIN_VALUE);
      assertTrue(testItem.compareTo(tdLesser) > 0);
    }
  }
  
  @Test
  public void cancelTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    assertEquals(future.cancel(true), testItem.cancel(true));
    assertEquals(future.cancel(false), testItem.cancel(false));
  }
  
  @Test
  public void isCancelledTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    assertEquals(future.isCancelled(), testItem.isCancelled());
    
    future.cancel(true);  // cancel through future
    
    assertTrue(testItem.isCancelled());
    
    future = new TestFutureImp(false);
    testItem = new ScheduledFutureDelegate<>(future, null);
    
    assertEquals(future.isCancelled(), testItem.isCancelled());
    
    testItem.cancel(true);  // cancel through delegate
    
    assertTrue(testItem.isCancelled());
    assertTrue(future.isCancelled());
  }
  
  @Test
  public void isDoneTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    future.cancel(true);
    
    assertEquals(future.isDone(), testItem.isDone());
  }

  @Test
  public void getTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    assertTrue(future.get() == testItem.get());
    
    assertTrue(future.get(10, TimeUnit.MILLISECONDS) == testItem.get(10, TimeUnit.MILLISECONDS));
  }

  @Test (expected = ExecutionException.class)
  public void getExecutionExceptionTest() throws InterruptedException, ExecutionException {
    TestFutureImp future = new TestFutureImp(false) {
      @Override
      public Object get() throws ExecutionException {
        throw new ExecutionException(new SuppressedStackRuntimeException());
      }
    };
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    testItem.get();
    fail("Exception should have thrown");
  }

  @Test (expected = TimeoutException.class)
  public void getTimeoutExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestFutureImp future = new TestFutureImp(false) {
      @Override
      public Object get(long timeout, TimeUnit unit) throws TimeoutException {
        throw new TimeoutException();
      }
    };
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    testItem.get(DELAY_TIME, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }

  @Test
  public void listenerTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    TestRunnable firstListener = new TestRunnable();
    TestRunnable secondListener = new TestRunnable();
    future.listener(firstListener);
    testItem.listener(secondListener);
    
    assertEquals(2, future.listeners.size());
    assertTrue(future.listeners.contains(firstListener));
    assertTrue(future.listeners.contains(secondListener));
  }

  @Test
  public void listenerExecutorTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    TestRunnable firstListener = new TestRunnable();
    TestRunnable secondListener = new TestRunnable();
    future.listener(firstListener, null);
    testItem.listener(secondListener, null);
    
    assertEquals(2, future.listeners.size());
    assertTrue(future.listeners.contains(firstListener));
    assertTrue(future.listeners.contains(secondListener));
  }
  
  @Test
  public void callbackAlreadyDoneTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    testItem.callback(new TestFutureCallback());
    
    assertEquals(0, future.listeners.size());
  }
  
  @Test
  public void callbackExecutorTest() {
    TestFutureImp future = new TestFutureImp(false);
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<>(future, null);
    
    testItem.callback(new TestFutureCallback(), 
                      SameThreadSubmitterExecutor.instance()); // trick so already-done optimization does not kick in
    
    assertEquals(1, future.listeners.size());
  }
}
