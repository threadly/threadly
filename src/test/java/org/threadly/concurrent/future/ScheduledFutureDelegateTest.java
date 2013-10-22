package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.TestDelayed;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ScheduledFutureDelegateTest<T> {
  @Test
  public void getDelayTest() {
    for(int i = -10; i <= 10; i++) {
      TestDelayed td = new TestDelayed(i);
      ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(null, td);
      
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
    for(int i = -10; i <= 10; i++) {
      TestDelayed td = new TestDelayed(i);
      ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(null, td);
      
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
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    assertEquals(future.cancel(true), testItem.cancel(true));
    assertEquals(future.cancel(false), testItem.cancel(false));
  }
  
  @Test
  public void isCancelledTest() {
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    assertEquals(future.isCancelled(), testItem.isCancelled());
    
    future.cancel(true);  // cancel through future
    
    assertTrue(testItem.isCancelled());
    
    future = new TestFutureImp();
    testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    assertEquals(future.isCancelled(), testItem.isCancelled());
    
    testItem.cancel(true);  // cancel through delegate
    
    assertTrue(testItem.isCancelled());
    assertTrue(future.isCancelled());
  }
  
  @Test
  public void isDoneTest() {
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    future.cancel(true);
    
    assertEquals(future.isDone(), testItem.isDone());
  }

  @Test
  public void getTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    assertTrue(future.get() == testItem.get());
    
    assertTrue(future.get(10, TimeUnit.MILLISECONDS) == testItem.get(10, TimeUnit.MILLISECONDS));
  }

  @Test (expected = ExecutionException.class)
  public void getExecutionExceptionTest() throws InterruptedException, ExecutionException {
    TestFutureImp future = new TestFutureImp() {
      @Override
      public Object get() throws ExecutionException {
        throw new ExecutionException(new RuntimeException());
      }
    };
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    testItem.get();
    fail("Exception should have thrown");
  }

  @Test (expected = TimeoutException.class)
  public void getTimeoutExceptionTest() throws InterruptedException, ExecutionException, TimeoutException {
    TestFutureImp future = new TestFutureImp() {
      @Override
      public Object get(long timeout, TimeUnit unit) throws TimeoutException {
        throw new TimeoutException();
      }
    };
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    testItem.get(10, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }

  @Test
  public void addListenerTest() {
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    TestRunnable firstListener = new TestRunnable();
    TestRunnable secondListener = new TestRunnable();
    future.addListener(firstListener);
    testItem.addListener(secondListener);
    
    assertEquals(2, future.listeners.size());
    assertTrue(future.listeners.contains(firstListener));
    assertTrue(future.listeners.contains(secondListener));
  }

  @Test
  public void addListenerExecutorTest() {
    TestFutureImp future = new TestFutureImp();
    ScheduledFutureDelegate<?> testItem = new ScheduledFutureDelegate<Object>(future, null);
    
    TestRunnable firstListener = new TestRunnable();
    TestRunnable secondListener = new TestRunnable();
    future.addListener(firstListener, null);
    testItem.addListener(secondListener, null);
    
    assertEquals(2, future.listeners.size());
    assertTrue(future.listeners.contains(firstListener));
    assertTrue(future.listeners.contains(secondListener));
  }
}
