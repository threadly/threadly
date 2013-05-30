package org.threadly.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeFutureTaskWrapper;
import org.threadly.concurrent.FutureTest.FutureFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorOneTimeFutureTaskWrapperTest {
  @Test
  public void blockTillCompletedTest() {
    FutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test
  public void blockTillCompletedFail() {
    FutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test
  public void getTimeoutFail() throws InterruptedException, ExecutionException {
    FutureTest.getTimeoutFail(new Factory());
  }
  
  @Test
  public void cancelTest() {
    FutureTest.cancelTest(new Factory());
  }
  
  @Test
  public void isDoneTest() {
    FutureTest.isDoneTest(new Factory());
  }
  
  @Test
  public void isDoneFail() {
    FutureTest.isDoneFail(new Factory());
  }
  
  private class Factory implements FutureFactory {
    @Override
    public Future<?> make(Runnable run, VirtualLock lock) {
      return new OneTimeFutureTaskWrapper<Object>(run, TaskPriority.High, 0, lock);
    }

    @Override
    public <T> Future<T> make(Callable<T> callable, VirtualLock lock) {
      return new OneTimeFutureTaskWrapper<T>(callable, TaskPriority.High, 0, lock);
    }
  }
}
