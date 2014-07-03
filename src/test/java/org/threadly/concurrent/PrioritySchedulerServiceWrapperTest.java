package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PrioritySchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  private static final int KEEP_ALIVE_TIME = 1000;
  
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    PriorityScheduler executor = new StrictPriorityScheduler(poolSize, poolSize, 
                                                             KEEP_ALIVE_TIME);
    return new PrioritySchedulerServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new PrioritySchedulerServiceWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void listenableFutureTest() {
    PriorityScheduler executor = new StrictPriorityScheduler(1, 1, 200);
    try {
      PrioritySchedulerServiceWrapper wrapper = new PrioritySchedulerServiceWrapper(executor);
      TestRunnable futureListener = new TestRunnable();
      ListenableFuture<?> future = wrapper.submit(new TestRunnable());
      future.addListener(futureListener);
      
      futureListener.blockTillFinished(); // throws exception if never called
    } finally {
      executor.shutdownNow();
    }
  }
}
