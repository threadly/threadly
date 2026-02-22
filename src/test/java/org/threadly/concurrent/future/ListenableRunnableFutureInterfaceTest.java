package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public abstract class ListenableRunnableFutureInterfaceTest extends CompletableListenableFutureInterfaceTest {
  protected abstract ListenableRunnableFutureFactory makeRunnableFutureFactory();
  
  @Override
  protected ListenableRunnableFutureFactory makeCompletableListenableFutureFactory() {
    return makeRunnableFutureFactory();
  }
  
  @Test
  public void getCallableResultTest() throws InterruptedException, ExecutionException {
    ListenableRunnableFutureFactory ff = makeRunnableFutureFactory();
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
    ListenableRunnableFutureFactory ff = makeRunnableFutureFactory();
    final Object result = new Object();
    RunnableFuture<Object> future = ff.make(DoNothingRunnable.instance(), result);
    
    future.run();
    
    assertTrue(future.get() == result);
  }

  @Test
  public void isDoneTest() {
    ListenableRunnableFutureFactory ff = makeRunnableFutureFactory();
    TestRunnable r = new TestRunnable();
    RunnableFuture<?> future = ff.make(r);
    future.run();
    
    assertTrue(future.isDone());
  }

  @Test
  public void isDoneFail() {
    ListenableRunnableFutureFactory ff = makeRunnableFutureFactory();
    TestRunnable r = new TestRuntimeFailureRunnable();
    RunnableFuture<?> future = ff.make(r);
    
    future.run();
    
    assertTrue(future.isDone());
  }
  
  @Test
  public void getAsyncResultTest() throws InterruptedException, ExecutionException {
    final String testResult = StringUtils.makeRandomString(5);
    RunnableFuture<String> lf = makeRunnableFutureFactory().make(() -> testResult);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
    try {
      scheduler.schedule(lf, DELAY_TIME);
      
      assertTrue(lf.get() == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAsyncResultWithTimeoutTest() throws InterruptedException, 
                                                     ExecutionException, 
                                                     TimeoutException {
    final String testResult = StringUtils.makeRandomString(5);
    RunnableFuture<String> lf = makeRunnableFutureFactory().make(() -> testResult);
    
    PriorityScheduler scheduler = new StrictPriorityScheduler(1);
    try {
      scheduler.prestartAllThreads();
      scheduler.schedule(lf, DELAY_TIME);
      
      assertTrue(lf.get(DELAY_TIME + (SLOW_MACHINE ? 2000 : 1000), TimeUnit.MILLISECONDS) == testResult);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  protected interface ListenableRunnableFutureFactory extends CompletableListenableFutureFactory {
    public RunnableFuture<?> make(Runnable run);
    public <T> RunnableFuture<T> make(Runnable run, T result);
    public <T> RunnableFuture<T> make(Callable<T> callable);
  }
}
