package org.threadly.test.concurrent;

import org.junit.Ignore;  // ignored due to never unblocking
import org.junit.Test;
import org.threadly.concurrent.ExecuteFuture;
import org.threadly.concurrent.ExecuteFutureTest;
import org.threadly.concurrent.ExecuteFutureTest.FutureFactory;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerOneTimeFutureRunnableTest {
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
      return new NoThreadScheduler().new OneTimeFutureRunnable(run, 0, lock);
    }
  }
}
