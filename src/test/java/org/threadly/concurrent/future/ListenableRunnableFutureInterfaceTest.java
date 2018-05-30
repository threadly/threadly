package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public abstract class ListenableRunnableFutureInterfaceTest extends ListenableFutureInterfaceTest {
  protected abstract ExecuteOnGetFutureFactory makeFutureFactory();
  
  @Override
  protected ListenableFutureFactory makeListenableFutureFactory() {
    return makeFutureFactory();
  }
  
  @Test
  public void getCallableResultTest() throws InterruptedException, ExecutionException {
    ExecuteOnGetFutureFactory ff = makeFutureFactory();
    final Object result = new Object();
    RunnableFuture<Object> future = ff.make(new Callable<Object>() {
      @Override
      public Object call() {
        return result;
      }
    });
    
    future.run();
    
    assertTrue(future.get() == result);
  }

  @Test
  public void getRunnableResultTest() throws InterruptedException, ExecutionException {
    ExecuteOnGetFutureFactory ff = makeFutureFactory();
    final Object result = new Object();
    RunnableFuture<Object> future = ff.make(DoNothingRunnable.instance(), result);
    
    future.run();
    
    assertTrue(future.get() == result);
  }

  @Test
  public void isDoneTest() {
    ExecuteOnGetFutureFactory ff = makeFutureFactory();
    TestRunnable r = new TestRunnable();
    RunnableFuture<?> future = ff.make(r);
    future.run();
    
    assertTrue(future.isDone());
  }

  @Test
  public void isDoneFail() {
    ExecuteOnGetFutureFactory ff = makeFutureFactory();
    TestRunnable r = new TestRuntimeFailureRunnable();
    RunnableFuture<?> future = ff.make(r);
    
    future.run();
    
    assertTrue(future.isDone());
  }

  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    ExecuteOnGetFutureFactory ff = makeFutureFactory();
    TestRunnable tr = new TestRunnable();
    RunnableFuture<?> future = ff.make(tr);
    
    // we never run the future, so we have to timeout
    
    long startTime = Clock.accurateForwardProgressingMillis();
    try {
      future.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (TimeoutException e) {
      long catchTime = Clock.accurateForwardProgressingMillis();
      assertTrue(catchTime - startTime >= DELAY_TIME);
    }
  }
  
  protected interface ExecuteOnGetFutureFactory extends ListenableFutureFactory {
    public RunnableFuture<?> make(Runnable run);
    public <T> RunnableFuture<T> make(Runnable run, T result);
    public <T> RunnableFuture<T> make(Callable<T> callable);
  }
}
