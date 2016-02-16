package org.threadly.concurrent.statistics;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Pair;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest extends PrioritySchedulerTest {
  @Override
  protected PrioritySchedulerServiceFactory getPrioritySchedulerFactory() {
    return new PrioritySchedulerStatisticTrackerTestFactory();
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorTest() {
    new PrioritySchedulerStatisticTracker(1);
    new PrioritySchedulerStatisticTracker(1, false);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, false);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 
                                          new ConfigurableThreadFactory());
  }
  
  @Test
  @Override
  public void constructorNullFactoryTest() {
    // ignored due to workerPool visibility
  }
  
  @SuppressWarnings("unused")
  @Test
  @Override
  public void constructorFail() {
    try {
      new PrioritySchedulerStatisticTracker(0, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PrioritySchedulerStatisticTracker(1, TaskPriority.High, -1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @Override
  public void removeHighPriorityCallableTest() {
    removeCallableTest(TaskPriority.High);
  }

  @Test
  @Override
  public void removeLowPriorityCallableTest() {
    removeCallableTest(TaskPriority.Low);
  }
  
  public static void removeCallableTest(TaskPriority priority) {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      TestCallable task = new TestCallable();
      scheduler.submitScheduled(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  // tests for statistics tracking
  
  private static void blockTillSchedulerIdle(final PrioritySchedulerStatisticTracker scheduler, 
                                             final int expectedSampleSize) {
    new TestCondition() { // block till all are finished
      @Override
      public boolean get() {
        return scheduler.getActiveTaskCount() == 0 && 
                 scheduler.getExecutionDurationSamples().size() >= expectedSampleSize;
      }
    }.blockTillTrue();
  }
  
  @Test
  public void resetCollectedStatsTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      // prestart so reuse percent is not zero
      scheduler.prestartAllThreads();
      TestRunnable lastRunnable = null;
      boolean flip = false;
      for (int i = 0; i < TEST_QTY; i++) {
        lastRunnable = new TestRunnable(1);
        if (flip) {
          scheduler.execute(lastRunnable, TaskPriority.High);
          flip = false;
        } else {
          scheduler.execute(lastRunnable, TaskPriority.Low);
          flip = true;
        }
      }
      
      lastRunnable.blockTillFinished();
      // block till all are finished
      blockTillSchedulerIdle(scheduler, TEST_QTY);
      
      // reset stats
      scheduler.resetCollectedStats();
      
      assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
      for (TaskPriority p : TaskPriority.values()) {
        assertEquals(-1, scheduler.getAverageExecutionDelay(p), 0);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    final int starvablePriorityCount = 1;
    final int lowPriorityCount = TEST_QTY;
    final int highPriorityCount = TEST_QTY * 2;
    final PrioritySchedulerStatisticTracker scheduler;
    scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      TestRunnable lastStarvablePriorityRunnable = null;
      for (int i = 0; i < starvablePriorityCount; i++) {
        lastStarvablePriorityRunnable = new TestRunnable();
        scheduler.execute(lastStarvablePriorityRunnable, TaskPriority.Starvable);
      }
      TestRunnable lastLowPriorityRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        lastLowPriorityRunnable = new TestRunnable();
        scheduler.execute(lastLowPriorityRunnable, TaskPriority.Low);
      }
      TestRunnable lastHighPriorityRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastHighPriorityRunnable = new TestRunnable();
        scheduler.execute(lastHighPriorityRunnable, TaskPriority.High);
      }
      lastLowPriorityRunnable.blockTillFinished();
      lastHighPriorityRunnable.blockTillFinished();
      
      // should not be very long after waiting above
      blockTillSchedulerIdle(scheduler, 
                             starvablePriorityCount + lowPriorityCount + highPriorityCount);
      
      assertEquals(starvablePriorityCount, scheduler.getTotalExecutionCount(TaskPriority.Starvable));
      assertEquals(lowPriorityCount, scheduler.getTotalExecutionCount(TaskPriority.Low));
      assertEquals(highPriorityCount, scheduler.getTotalExecutionCount(TaskPriority.High));
      assertEquals(starvablePriorityCount + lowPriorityCount + highPriorityCount, 
                   scheduler.getTotalExecutionCount(null));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(-1, scheduler.getAverageExecutionDelay(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }

  public void getPriorityAverageExecutionDelayNoInputTest(TaskPriority testPriority) {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      BlockingTestRunnable br = new BlockingTestRunnable();
      switch (testPriority) { // submit with opposite priority
        case High:
          scheduler.execute(br, TaskPriority.Low);
          break;
        case Low:
          scheduler.execute(br, TaskPriority.High);
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + testPriority);
      }
      br.unblock();

      // wait for task to finish now
      br.blockTillFinished();
      blockTillSchedulerIdle(scheduler, 1);
      
      assertEquals(-1, scheduler.getAverageExecutionDelay(testPriority), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAverageExecutionDelayNoInputTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAverageExecutionDelayNoInputTest(TaskPriority.Low);
  }

  protected void getPriorityAverageExecutionDelayTest(TaskPriority priority) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
      List<Long> samples = scheduler.getExecutionDelaySamples(priority);
      
      double total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }
      
      double expectedAvg = total / samples.size();

      assertEquals(expectedAvg, scheduler.getAverageExecutionDelay(priority), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityAverageExecutionDelayTest() {
    getPriorityAverageExecutionDelayTest(null);
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    getPriorityAverageExecutionDelayTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    getPriorityAverageExecutionDelayTest(TaskPriority.Low);
  }
  
  @Test
  public void getExecutionDelayPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
      scheduler.execute(btr);
      scheduler.execute(new ClockUpdateRunnable());
      TestUtils.blockTillClockAdvances();
      btr.unblock();
      assertEquals(1, scheduler.getExecutionDelayPercentiles(90).get(90.), 2);
    } finally {
      btr.unblock();
      scheduler.shutdownNow();
    }
  }
  
  protected void getPriorityExecutionDelayPercentilesTest(TaskPriority priority) {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
      scheduler.execute(btr);
      scheduler.execute(new ClockUpdateRunnable(), priority);
      TestUtils.blockTillClockAdvances();
      btr.unblock();
      assertEquals(1, scheduler.getExecutionDelayPercentiles(90).get(90.), 2);
    } finally {
      btr.unblock();
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityExecutionDelayPercentilesTest() {
    getPriorityExecutionDelayPercentilesTest(null);
  }
  
  @Test
  public void getHighPriorityExecutionDelayPercentilesTest() {
    getPriorityExecutionDelayPercentilesTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityExecutionDelayPercentilesTest() {
    getPriorityExecutionDelayPercentilesTest(TaskPriority.Low);
  }
  
  @Test
  public void getAverageExecutionDurationNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getAverageExecutionDurationSimpleTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
      scheduler.execute(new ClockUpdateRunnable());
      blockTillSchedulerIdle(scheduler, 1);
      assertEquals(1, scheduler.getAverageExecutionDuration(), 1);
      scheduler.execute(new ClockUpdateRunnable(DELAY_TIME));
      blockTillSchedulerIdle(scheduler, 2);
      assertTrue(scheduler.getAverageExecutionDuration() >= DELAY_TIME / 2);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDurationTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      for (int i = 0; i < lowPriorityCount; i++) {
        scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
      }
      TestRunnable lastRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastRunnable = new TestRunnable();
        scheduler.execute(lastRunnable, TaskPriority.High);
      }
      
      lastRunnable.blockTillFinished();
      int expectedCount = lowPriorityCount + highPriorityCount;
      // block till all are finished
      blockTillSchedulerIdle(scheduler, expectedCount);
      
      List<Long> runTimes = scheduler.getExecutionDurationSamples();
      assertEquals(expectedCount, runTimes.size());
      
      double totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      double avgRunTime = totalRunTime / runTimes.size();
      
      assertEquals(avgRunTime, scheduler.getAverageExecutionDuration(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
      assertEquals(-1, scheduler.getAverageExecutionDuration(TaskPriority.High), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      for (int i = 0; i < lowPriorityCount; i++) {
        scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
      }
      TestRunnable lastRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastRunnable = new TestRunnable();
        scheduler.execute(lastRunnable, TaskPriority.High);
      }
      
      lastRunnable.blockTillFinished();
      // block till all are finished
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
      List<Long> runTimes = scheduler.getExecutionDurationSamples(TaskPriority.High);
      assertEquals(highPriorityCount, runTimes.size());
      
      double totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      double avgRunTime = totalRunTime / runTimes.size();
      
      assertEquals(avgRunTime, scheduler.getAverageExecutionDuration(TaskPriority.High), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getExecutionDurationPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      assertEquals(0, scheduler.getExecutionDurationPercentiles(50).get(50.), 0);
      scheduler.execute(new ClockUpdateRunnable());
      blockTillSchedulerIdle(scheduler, 1);
      assertEquals(1, scheduler.getExecutionDurationPercentiles(50).get(50.), 1);
      scheduler.execute(new ClockUpdateRunnable());
      scheduler.execute(new ClockUpdateRunnable());
      scheduler.execute(new ClockUpdateRunnable());
      scheduler.execute(new ClockUpdateRunnable(DELAY_TIME));
      blockTillSchedulerIdle(scheduler, 5);
      assertEquals(1, scheduler.getExecutionDurationPercentiles(75).get(75.), 1);
      assertTrue(scheduler.getExecutionDurationPercentiles(90).get(90.) >= DELAY_TIME);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getWithPriorityExecutionDurationPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      assertEquals(0, scheduler.getExecutionDurationPercentiles(TaskPriority.High, 50).get(50.), 0);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.Low);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.Low);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.Low);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.Low);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.Low);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.High);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.High);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.High);
      scheduler.execute(new ClockUpdateRunnable(), TaskPriority.High);
      scheduler.execute(new ClockUpdateRunnable(DELAY_TIME), TaskPriority.High);
      blockTillSchedulerIdle(scheduler, 10);
      assertEquals(1, scheduler.getExecutionDurationPercentiles(TaskPriority.High, 75).get(75.), 1);
      assertTrue(scheduler.getExecutionDurationPercentiles(TaskPriority.High, 90).get(90.) >= DELAY_TIME);
      assertEquals(1, scheduler.getExecutionDurationPercentiles(TaskPriority.Low, 90).get(90.), 1);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksTest() {
    final int checkTime = DELAY_TIME;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 100, true);
    try {
      BlockingTestRunnable br = new BlockingTestRunnable();
      scheduler.execute(br);
      
      br.blockTillStarted();
      TestUtils.sleep(checkTime + 1);
      
      assertEquals(1, scheduler.getLongRunningTasksQty(checkTime));
      List<Pair<Runnable, StackTraceElement[]>> longRunning = scheduler.getLongRunningTasks(checkTime);
      br.unblock();
      
      assertEquals(1, longRunning.size());
      assertTrue(longRunning.get(0).getLeft() == br);
      
      // wait for task to finish now
      blockTillSchedulerIdle(scheduler, 1);

      assertEquals(0, scheduler.getLongRunningTasksQty(0));
      longRunning = scheduler.getLongRunningTasks(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksWrappedFutureTest() throws InterruptedException, TimeoutException {
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      final AsyncVerifier av = new AsyncVerifier();
      scheduler.submit(new ClockUpdateRunnable() {
        @Override
        public void handleRunStart() {
          // even submitted (and thus wrapped in a future), we should get our direct reference
          av.assertTrue(scheduler.getLongRunningTasks(-1).get(0).getLeft() == this);
          av.signalComplete();
        }
      });
      av.waitForTest();
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksQtyTest() {
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(0, scheduler.getLongRunningTasksQty(-1));
      scheduler.execute(new ClockUpdateRunnable() {
        @Override
        public void handleRunStart() {
          assertEquals(1, scheduler.getLongRunningTasksQty(-1));
          assertEquals(0, scheduler.getLongRunningTasksQty(10));
        }
      });
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  private class PrioritySchedulerStatisticTrackerTestFactory implements PrioritySchedulerServiceFactory {
    private final List<PriorityScheduler> executors;
    
    private PrioritySchedulerStatisticTrackerTestFactory() {
      executors = new LinkedList<PriorityScheduler>();
    }

    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = makePriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartAllThreads();
      }
      
      return result;
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize,
                                                                   TaskPriority defaultPriority,
                                                                   long maxWaitForLowPriority) {
      return makePriorityScheduler(poolSize, defaultPriority, maxWaitForLowPriority);
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize) {
      return makePriorityScheduler(poolSize);
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int poolSize, TaskPriority defaultPriority,
                                                   long maxWaitForLowPriority) {
      PriorityScheduler result = new PrioritySchedulerStatisticTracker(poolSize, defaultPriority, 
                                                                       maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int poolSize) {
      PriorityScheduler result = new PrioritySchedulerStatisticTracker(poolSize);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
}
