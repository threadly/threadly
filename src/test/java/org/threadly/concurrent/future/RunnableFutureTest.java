package org.threadly.concurrent.future;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableFutureTest {
  private static PriorityScheduledExecutor scheduler;
  
  static {
    scheduler = new PriorityScheduledExecutor(10, 10, 500);
    
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  public static void getTimeoutFail(FutureFactory ff) throws InterruptedException, 
                                                             ExecutionException {
    final int threadSleepTime = DELAY_TIME * 1000;

    TestRunnable tr = new TestRunnable(threadSleepTime);
    Future<?> future = ff.make(tr);
    
    scheduler.execute((Runnable)future);
    
    long startTime = System.currentTimeMillis();
    try {
      future.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have been thrown");
    } catch (TimeoutException e) {
      long catchTime = System.currentTimeMillis();
      assertTrue(catchTime - startTime >= DELAY_TIME);
    }
  }
  
  public static void isDoneTest(FutureFactory ff) {
    TestRunnable r = new TestRunnable();
    RunnableFuture<?> future = ff.make(r);
    scheduler.execute(future);
    try {
      future.get();
    } catch (InterruptedException e) {
      // ignored
    } catch (ExecutionException e) {
      // ignored
    }
    
    assertTrue(future.isDone());
  }
  
  public static void isDoneFail(FutureFactory ff) {
    TestRunnable r = new TestRuntimeFailureRunnable();
    RunnableFuture<?> future = ff.make(r);
    scheduler.execute(future);
    try {
      future.get();
    } catch (InterruptedException e) {
      // ignored
    } catch (ExecutionException e) {
      // ignored
    }
    
    assertTrue(future.isDone());
  }
  
  public interface FutureFactory {
    public RunnableFuture<?> make(Runnable run);
    public <T> RunnableFuture<T> make(Callable<T> callable);
  }
}
