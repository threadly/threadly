package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;

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
    return new ExecuteOnGetFutureTask<T>(runnable, result);
  }

  @Override
  protected <T> ListenableFutureTask<T> makeFutureTask(Callable<T> task) {
    return new ExecuteOnGetFutureTask<T>(task);
  }
  
  @Test
  public void executeInvokedByGetTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ExecuteOnGetFutureTask<?> geft = new ExecuteOnGetFutureTask<Void>(tr);
    
    geft.get();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void executeOnceTest() throws InterruptedException, ExecutionException {
    TestRunnable tr = new TestRunnable();
    ExecuteOnGetFutureTask<?> geft = new ExecuteOnGetFutureTask<Void>(tr);
    
    geft.get();
    geft.run();
    geft.get(); // multiple get calls should not execute again either
    geft.run();
    
    assertTrue(tr.ranOnce());
  }
  
  private class Factory implements ExecuteOnGetFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run) {
      return new ExecuteOnGetFutureTask<Object>(run);
    }

    @Override
    public <T> RunnableFuture<T> make(Runnable run, T result) {
      return new ExecuteOnGetFutureTask<T>(run, result);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return new ExecuteOnGetFutureTask<T>(callable);
    }

    @Override
    public ListenableFuture<?> makeCanceled() {
      ListenableFutureTask<?> lft = new ExecuteOnGetFutureTask<Object>(DoNothingRunnable.instance());
      lft.cancel(false);
      return lft;
    }

    @Override
    public ListenableFuture<?> makeWithFailure(Exception t) {
      ListenableFutureTask<?> lft = new ExecuteOnGetFutureTask<Object>(() -> { throw t; });
      lft.run();
      return lft;
    }

    @Override
    public <T> ListenableFuture<T> makeWithResult(T result) {
      ListenableFutureTask<T> lft = new ExecuteOnGetFutureTask<T>(() -> result);
      lft.run();
      return lft;
    }
  }
}
