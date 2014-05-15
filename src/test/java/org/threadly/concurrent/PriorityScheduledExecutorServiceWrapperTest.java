package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest extends ScheduledExecutorServiceTest {
  private static final int KEEP_ALIVE_TIME = 1000;
  
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                             KEEP_ALIVE_TIME);
    return new PriorityScheduledExecutorServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new PriorityScheduledExecutorServiceWrapper(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void listenableFutureTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 200);
    try {
      PriorityScheduledExecutorServiceWrapper wrapper = new PriorityScheduledExecutorServiceWrapper(executor);
      TestRunnable futureListener = new TestRunnable();
      ListenableFuture<?> future = wrapper.submit(new TestRunnable());
      future.addListener(futureListener);
      
      futureListener.blockTillFinished(); // throws exception if never called
    } finally {
      executor.shutdownNow();
    }
  }
}
