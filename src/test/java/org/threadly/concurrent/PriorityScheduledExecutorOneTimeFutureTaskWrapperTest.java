package org.threadly.concurrent;

import org.junit.Ignore;  // ignored due to never unblocking
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeFutureTaskWrapper;
import org.threadly.concurrent.ExecuteFutureTest.FutureFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorOneTimeFutureTaskWrapperTest {
  @Test @Ignore
  public void blockTillCompletedTest() {
    ExecuteFutureTest.blockTillCompletedTest(new Factory());
  }
  
  @Test @Ignore
  public void blockTillCompletedFail() {
    ExecuteFutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test @Ignore
  public void isCompletedTest() {
    ExecuteFutureTest.blockTillCompletedFail(new Factory());
  }
  
  @Test @Ignore
  public void isCompletedFail() {
    ExecuteFutureTest.blockTillCompletedFail(new Factory());
  }
  
  private class Factory implements FutureFactory {
    @Override
    public ExecuteFuture make(Runnable run, VirtualLock lock) {
      return new OneTimeFutureTaskWrapper(run, TaskPriority.High, 0, lock);
    }
  }
}
