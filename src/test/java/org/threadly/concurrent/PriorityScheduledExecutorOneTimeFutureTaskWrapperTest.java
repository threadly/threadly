package org.threadly.concurrent;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeFutureTaskWrapper;
import org.threadly.concurrent.ExecuteFutureTest.FutureFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorOneTimeFutureTaskWrapperTest {
  @Test
  public void blockTillCompletedTest() {
    ExecuteFutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test
  public void blockTillCompletedFail() {
    ExecuteFutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test
  public void isCompletedTest() {
    ExecuteFutureTest.isCompletedTest(new Factory());
  }
  
  @Test
  public void isCompletedFail() {
    ExecuteFutureTest.isCompletedFail(new Factory());
  }
  
  private class Factory implements FutureFactory {
    @Override
    public ExecuteFuture make(Runnable run, VirtualLock lock) {
      return new OneTimeFutureTaskWrapper(run, TaskPriority.High, 0, lock);
    }
  }
}
