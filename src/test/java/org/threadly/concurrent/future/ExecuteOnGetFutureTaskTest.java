package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecuteOnGetFutureTaskTest extends ListenableFutureTaskTest {
  @Override
  protected ExecuteOnGetFutureFactory makeFutureFactory() {
    return new Factory();
  }

  @Override
  protected <T> ListenableFutureTask<T> makeFutureTask(Runnable runnable, T result) {
    return new ExecuteOnGetFutureTask<>(runnable, result);
  }

  @Override
  protected <T> ListenableFutureTask<T> makeFutureTask(Callable<T> task) {
    return new ExecuteOnGetFutureTask<>(task);
  }
  
  @Test
  public void executeInvokedByGetTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ExecuteOnGetFutureTask<?> geft = new ExecuteOnGetFutureTask<>(tr);
    
    geft.get();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void executeInvokedByGetFailureTest() throws InterruptedException {
    TestRunnable tr = new TestRunnable();
    ExecuteOnGetFutureTask<?> geft = new ExecuteOnGetFutureTask<>(tr);
    
    assertNull(geft.getFailure());
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void executeOnceTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ExecuteOnGetFutureTask<?> geft = new ExecuteOnGetFutureTask<>(tr);
    
    geft.get();
    geft.run();
    geft.get(); // multiple get calls should not execute again either
    geft.getFailure();
    geft.run();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  @Override
  public void mapStackSizeTest() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    ListenableFutureInterfaceTest.mapStackDepthTest(future, future, 57, 37);
  }
  
  @Test
  @Override
  public void mapFailureStackSize() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(() -> { throw new RuntimeException(); }, null);
    ListenableFutureInterfaceTest.mapFailureStackDepthTest(future, future, 57);
  }
  
  @Test
  @Override
  public void flatMapStackSizeTest() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    ListenableFutureInterfaceTest.flatMapStackDepthTest(future, future, 77, 15);
  }
  
  private class Factory implements ExecuteOnGetFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ExecuteOnGetFutureTask<>(run);
    }

    @Override
    public <T> RunnableFuture<T> make(Runnable run, T result) {
      return new ExecuteOnGetFutureTask<>(run, result);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ExecuteOnGetFutureTask<>(callable);
    }

    @Override
    public ListenableFuture<?> makeCanceled() {
      ListenableFutureTask<?> lft = new ExecuteOnGetFutureTask<>(DoNothingRunnable.instance());
      lft.cancel(false);
      return lft;
    }

    @Override
    public ListenableFuture<Object> makeWithFailure(Exception t) {
      ListenableFutureTask<Object> lft = new ExecuteOnGetFutureTask<>(() -> { throw t; });
      lft.run();
      return lft;
    }

    @Override
    public <T> ListenableFuture<T> makeWithResult(T result) {
      ListenableFutureTask<T> lft = new ExecuteOnGetFutureTask<>(() -> result);
      lft.run();
      return lft;
    }
  }
}
