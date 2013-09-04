package org.threadly.test.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.future.RunnableFutureTest;
import org.threadly.concurrent.future.RunnableFutureTest.BlockingFutureFactory;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.NoThreadScheduler.OneTimeFutureRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerOneTimeFutureRunnableTest {
  @Test
  public void blockTillCompletedTest() {
    RunnableFutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test
  public void blockTillCompletedFail() {
    RunnableFutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    RunnableFutureTest.getTimeoutFail(new Factory());
  }
  
  @Test
  public void cancelTest() {
    RunnableFutureTest.cancelTest(new Factory());
  }
  
  @Test
  public void isDoneTest() {
    RunnableFutureTest.isDoneTest(new Factory());
  }
  
  @Test
  public void isDoneFail() {
    RunnableFutureTest.isDoneFail(new Factory());
  }
  
  private class Factory implements BlockingFutureFactory {
    @Override
    public RunnableFuture<?> make(Runnable run, VirtualLock lock) {
      return new FutureRunnable<Object>(run, lock);
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable, VirtualLock lock) {
      return new FutureRunnable<T>(callable, lock);
    }

    @Override
    public RunnableFuture<?> make(Runnable run) {
      return make(run, new NativeLock());
    }

    @Override
    public <T> RunnableFuture<T> make(Callable<T> callable) {
      return make(callable, new NativeLock());
    }
  }
  
  private class FutureRunnable<T> implements RunnableFuture<T> {
    private final OneTimeFutureRunnable<T> otfr;
    
    private FutureRunnable(Runnable run, VirtualLock lock) {
      this.otfr = new NoThreadScheduler().new OneTimeFutureRunnable<T>(run, null, 0, lock);
    }
    
    private FutureRunnable(Callable<T> callable, VirtualLock lock) {
      this.otfr = new NoThreadScheduler().new OneTimeFutureRunnable<T>(callable, 0, lock);
    }

    @Override
    public void run() {
      otfr.run(System.currentTimeMillis());
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return otfr.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return otfr.isCancelled();
    }

    @Override
    public boolean isDone() {
      return otfr.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      return otfr.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
                                                     ExecutionException,
                                                     TimeoutException {
      return otfr.get(timeout, unit);
    }
  }
}
