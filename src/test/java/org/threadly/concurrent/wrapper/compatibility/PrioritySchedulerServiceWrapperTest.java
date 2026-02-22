package org.threadly.concurrent.wrapper.compatibility;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PrioritySchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    PriorityScheduler executor = new StrictPriorityScheduler(poolSize);
    return new PrioritySchedulerServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new PrioritySchedulerServiceWrapper(null);
      });
  }
  
  @Test
  public void listenableFutureTest() {
    PriorityScheduler executor = new StrictPriorityScheduler(1);
    try {
      PrioritySchedulerServiceWrapper wrapper = new PrioritySchedulerServiceWrapper(executor);
      TestRunnable futureListener = new TestRunnable();
      wrapper.submit(DoNothingRunnable.instance())
             .listener(futureListener);
      
      futureListener.blockTillFinished(); // throws exception if never called
    } finally {
      executor.shutdownNow();
    }
  }
}
