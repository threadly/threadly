package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableFutureTest {
  public static void getCallableResultTest(FutureFactory ff) throws InterruptedException, ExecutionException {
    final Object result = new Object();
    RunnableFuture<Object> future = ff.make(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return result;
      }
    });
    
    future.run();
    
    assertTrue(future.get() == result);
  }
  
  public static void getRunnableResultTest(FutureFactory ff) throws InterruptedException, ExecutionException {
    final Object result = new Object();
    RunnableFuture<Object> future = ff.make(new TestRunnable(), result);
    
    future.run();
    
    assertTrue(future.get() == result);
  }
  
  public static void isDoneTest(FutureFactory ff) {
    TestRunnable r = new TestRunnable();
    RunnableFuture<?> future = ff.make(r);
    future.run();
    
    assertTrue(future.isDone());
  }
  
  public static void isDoneFail(FutureFactory ff) {
    TestRunnable r = new TestRuntimeFailureRunnable();
    RunnableFuture<?> future = ff.make(r);
    
    future.run();
    
    assertTrue(future.isDone());
  }
  
  public static void getTimeoutFail(FutureFactory ff) throws InterruptedException, 
                                                             ExecutionException {
    TestRunnable tr = new TestRunnable();
    RunnableFuture<?> future = ff.make(tr);
    
    // we never run the future, so we have to timeout
    
    long startTime = System.currentTimeMillis();
    try {
      future.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (TimeoutException e) {
      long catchTime = System.currentTimeMillis();
      assertTrue(catchTime - startTime >= DELAY_TIME);
    }
  }
  
  public interface FutureFactory {
    public RunnableFuture<?> make(Runnable run);
    public <T> RunnableFuture<T> make(Runnable run, T result);
    public <T> RunnableFuture<T> make(Callable<T> callable);
  }
}
