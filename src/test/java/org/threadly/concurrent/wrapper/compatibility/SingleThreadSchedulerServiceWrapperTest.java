package org.threadly.concurrent.wrapper.compatibility;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    SingleThreadScheduler executor = new SingleThreadScheduler();
    return new SingleThreadSchedulerServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new SingleThreadSchedulerServiceWrapper(null);
      });
  }

  @Test
  @Override // must be overridden because we can only do this test with one task for the single threaded version
  public void scheduleAtFixedRateTest() {
    ScheduledExecutorService scheduler = makeScheduler(1);
    try {
      // execute a task first in case there are any initial startup actions which may be slow
      scheduler.execute(DoNothingRunnable.instance());

      TestRunnable tr = new TestRunnable(DELAY_TIME - 1);
      scheduler.scheduleAtFixedRate(tr, 0, DELAY_TIME, TimeUnit.MILLISECONDS);

      tr.blockTillFinished((DELAY_TIME * (CYCLE_COUNT - 1)) + 2000, CYCLE_COUNT);
      long executionDelay = tr.getDelayTillRun(CYCLE_COUNT);
      assertTrue(executionDelay >= DELAY_TIME * (CYCLE_COUNT - 1));
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (DELAY_TIME * (CYCLE_COUNT - 1)) + (SLOW_MACHINE ? 2000 : 1000));
    } finally {
      scheduler.shutdownNow();
    }
  }
}
