package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerServiceWrapperTest extends ScheduledExecutorServiceTest {
  @Override
  protected ScheduledExecutorService makeScheduler(int poolSize) {
    SingleThreadScheduler executor = new SingleThreadScheduler();
    return new SingleThreadSchedulerServiceWrapper(executor);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SingleThreadSchedulerServiceWrapper(null);
    fail("Exception should have thrown");
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
      assertTrue(executionDelay <= (DELAY_TIME * (CYCLE_COUNT - 1)) + 1000);
    } finally {
      scheduler.shutdownNow();
    }
  }
}
