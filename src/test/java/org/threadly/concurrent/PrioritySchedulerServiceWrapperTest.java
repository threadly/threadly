package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.wrapper.compatibility.ScheduledExecutorServiceTest;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings({"javadoc", "deprecation"})
public class PrioritySchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
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
    PriorityScheduler executor = new StrictPriorityScheduler(1);
    try {
      PrioritySchedulerServiceWrapper wrapper = new PrioritySchedulerServiceWrapper(executor);
      TestRunnable futureListener = new TestRunnable();
      ListenableFuture<?> future = wrapper.submit(DoNothingRunnable.instance());
      future.addListener(futureListener);
      
      futureListener.blockTillFinished(); // throws exception if never called
    } finally {
      executor.shutdownNow();
    }
  }
}
