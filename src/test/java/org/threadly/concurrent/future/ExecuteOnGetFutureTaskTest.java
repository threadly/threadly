package org.threadly.concurrent.future;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecuteOnGetFutureTaskTest extends ListenableFutureTaskTest {
  @Override
  protected ExecuteOnGetFutureFactory makeRunnableFutureFactory() {
    return new ExecuteOnGetFutureFactory();
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
    ListenableFutureInterfaceTest.mapStackDepthTest(future, future, 54, 37);
  }
  
  @Test
  @Override
  public void mapFailureStackSize() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(() -> { throw new RuntimeException(); }, null);
    ListenableFutureInterfaceTest.mapFailureStackDepthTest(future, future, 54);
  }
  
  @Test
  @Override
  public void flatMapStackSizeTest() throws InterruptedException, TimeoutException {
    ListenableFutureTask<Object> future = makeFutureTask(DoNothingRunnable.instance(), null);
    ListenableFutureInterfaceTest.flatMapStackDepthTest(future, future, 74, 15);
  }
  
  private class ExecuteOnGetFutureFactory implements ListenableRunnableFutureFactory {
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
    public <T> ExecuteOnGetFutureTask<T> makeNewCompletable() {
      return new ExecuteOnGetFutureTask<>(DoNothingRunnable.instance());
    }

    @Override
    public ExecuteOnGetFutureTask<?> makeCanceledCompletable() {
      ExecuteOnGetFutureTask<?> lft = new ExecuteOnGetFutureTask<>(DoNothingRunnable.instance());
      lft.cancel(false);
      return lft;
    }

    @Override
    public ExecuteOnGetFutureTask<Object> makeWithFailureCompletable(Exception t) {
      ExecuteOnGetFutureTask<Object> lft = new ExecuteOnGetFutureTask<>(() -> { throw t; });
      lft.run();
      return lft;
    }

    @Override
    public <T> ExecuteOnGetFutureTask<T> makeWithResultCompletable(T result) {
      ExecuteOnGetFutureTask<T> lft = new ExecuteOnGetFutureTask<>(() -> result);
      lft.run();
      return lft;
    }
  }
}
