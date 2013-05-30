package org.threadly.test.concurrent;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.concurrent.ExecuteFuture;
import org.threadly.concurrent.ExecuteFutureTest;
import org.threadly.concurrent.ExecuteFutureTest.FutureFactory;
import org.threadly.concurrent.lock.VirtualLock;
import org.threadly.test.concurrent.NoThreadScheduler.OneTimeFutureRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerOneTimeFutureRunnableTest {
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
      return new FutureRunnable(run, lock);
    }
  }
  
  private class FutureRunnable implements ExecuteFuture, Runnable {
    private final OneTimeFutureRunnable otfr;
    
    private FutureRunnable(Runnable run, VirtualLock lock) {
      this.otfr = new NoThreadScheduler().new OneTimeFutureRunnable(run, 0, lock);
    }

    @Override
    public void run() {
      otfr.run(System.currentTimeMillis());
    }

    @Override
    public void blockTillCompleted() throws InterruptedException,
                                    ExecutionException {
      otfr.blockTillCompleted();
    }

    @Override
    public boolean isCompleted() {
      return otfr.isCompleted();
    }
  }
}
