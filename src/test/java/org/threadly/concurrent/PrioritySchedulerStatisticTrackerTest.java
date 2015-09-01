package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest extends PrioritySchedulerTest {
  @Override
  protected PrioritySchedulerFactory getPrioritySchedulerFactory() {
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
  
  @Override
  @Test
  public void constructorNullFactoryTest() {
    PriorityScheduler ps = new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 1, null);
    // should be set with default
    assertNotNull(ps.workerPool.threadFactory);
  }
  
  @Override
  @SuppressWarnings("unused")
  @Test
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
  
  @Override
  @Test
  public void removeHighPriorityCallableTest() {
    removeCallableTest(TaskPriority.High);
  }
  
  @Override
  @Test
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
        return scheduler.getCurrentRunningCount() == 0 && 
                 scheduler.getRunTimes().size() >= expectedSampleSize;
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
      
      assertEquals(-1, scheduler.getAverageTaskRunTime(), 0);
      assertEquals(-1, scheduler.getHighPriorityAvgExecutionDelay(), 0);
      assertEquals(-1, scheduler.getLowPriorityAvgExecutionDelay(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(-1, scheduler.getAverageTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgRunTimeTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      for (int i = 0; i < lowPriorityCount; i++) {
        scheduler.execute(DoNothingRunnable.instance(), 
                          TaskPriority.Low);
      }
      TestRunnable lastRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastRunnable = new TestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      
      lastRunnable.blockTillFinished();
      int expectedCount = lowPriorityCount + highPriorityCount;
      // block till all are finished
      blockTillSchedulerIdle(scheduler, expectedCount);
      
      List<Long> runTimes = scheduler.getRunTimes();
      assertEquals(expectedCount, 
                   runTimes.size());
      
      double totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      long avgRunTime = Math.round(totalRunTime / runTimes.size());
      
      assertEquals(avgRunTime, scheduler.getAverageTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    final PrioritySchedulerStatisticTracker scheduler;
    scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      TestRunnable lastLowPriorityRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        lastLowPriorityRunnable = new TestRunnable();
        scheduler.execute(lastLowPriorityRunnable, 
                          TaskPriority.Low);
      }
      TestRunnable lastHighPriorityRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastHighPriorityRunnable = new TestRunnable();
        scheduler.execute(lastHighPriorityRunnable, 
                          TaskPriority.High);
      }
      lastLowPriorityRunnable.blockTillFinished();
      lastHighPriorityRunnable.blockTillFinished();
      
      // should not be very long after waiting above
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentRunningCount() == 0;
        }
      }.blockTillTrue();
      
      assertEquals(lowPriorityCount + highPriorityCount, 
                   scheduler.getTotalExecutionCount());
      assertEquals(lowPriorityCount, scheduler.getLowPriorityTotalExecutionCount());
      assertEquals(highPriorityCount, scheduler.getHighPriorityTotalExecutionCount());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(-1, scheduler.getMedianTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount);
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
      List<Long> samples = new ArrayList<Long>(scheduler.getRunTimes());
      Collections.sort(samples);
      
      assertEquals(0, scheduler.getMedianTaskRunTime(), samples.get(samples.size() / 2));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(-1, scheduler.getAvgExecutionDelay());
    } finally {
      scheduler.shutdownNow();
    }
  }

  public void getPriorityAvgExecutionDelayNoInputTest(TaskPriority testPriority) {
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

      switch (testPriority) {
        case High:
          assertEquals(-1, scheduler.getHighPriorityAvgExecutionDelay());
          break;
        case Low:
          assertEquals(-1, scheduler.getLowPriorityAvgExecutionDelay());
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + testPriority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAvgExecutionDelayNoInputTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAvgExecutionDelayNoInputTest(TaskPriority.Low);
  }
  

  public void getPriorityAvgExecutionDelayTest(TaskPriority priority) {
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
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
      List<Long> samples;
      switch (priority) {
        case High:
          samples = scheduler.getHighPriorityExecutionDelays();
          break;
        case Low:
          samples = scheduler.getLowPriorityExecutionDelays();
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
      
      double total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }
      
      long expectedAvg = Math.round(total / samples.size());

      switch (priority) {
        case High:
          assertEquals(expectedAvg, scheduler.getHighPriorityAvgExecutionDelay());
          break;
        case Low:
          assertEquals(expectedAvg, scheduler.getLowPriorityAvgExecutionDelay());
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    getPriorityAvgExecutionDelayTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    getPriorityAvgExecutionDelayTest(TaskPriority.Low);
  }
  

  public void getPriorityMedianExecutionDelayTest(TaskPriority priority) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);

    assertEquals(-1, scheduler.getHighPriorityMedianExecutionDelay());
    assertEquals(-1, scheduler.getLowPriorityMedianExecutionDelay());
    
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
      List<Long> samples;
      switch (priority) {
        case High:
          samples = new ArrayList<Long>(scheduler.getHighPriorityExecutionDelays());
          break;
        case Low:
          samples = new ArrayList<Long>(scheduler.getLowPriorityExecutionDelays());
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
      
      Collections.sort(samples);

      switch (priority) {
        case High:
          assertEquals(samples.get(samples.size() / 2), scheduler.getHighPriorityMedianExecutionDelay(), 0);
          break;
        case Low:
          assertEquals(samples.get(samples.size() / 2), scheduler.getLowPriorityMedianExecutionDelay(), 0);
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityMedianExecutionDelayTest() {
    getPriorityMedianExecutionDelayTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityMedianExecutionDelayTest() {
    getPriorityMedianExecutionDelayTest(TaskPriority.Low);
  }
  
  @Test
  public void getRunnablesRunningOverTimeTest() {
    final int checkTime = 20;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      BlockingTestRunnable br = new BlockingTestRunnable();
      scheduler.execute(br);
      
      br.blockTillStarted();
      TestUtils.sleep(checkTime + 1);
      
      assertEquals(1, scheduler.getQtyRunningOverTime(checkTime));
      List<Runnable> longRunning = scheduler.getRunnablesRunningOverTime(checkTime);
      br.unblock();
      
      assertEquals(1, longRunning.size());
      assertTrue(longRunning.get(0) == br);
      
      // wait for task to finish now
      blockTillSchedulerIdle(scheduler, 1);

      assertEquals(0, scheduler.getQtyRunningOverTime(0));
      longRunning = scheduler.getRunnablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getCallablesRunningOverTimeTest() {
    final int checkTime = 20;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      BlockCallable bc = new BlockCallable();
      scheduler.submit(bc);
      
      bc.blockTillStarted();
      TestUtils.sleep(checkTime + 1);

      assertEquals(1, scheduler.getQtyRunningOverTime(checkTime));
      List<Callable<?>> longRunning = scheduler.getCallablesRunningOverTime(checkTime);
      bc.unblock();
      
      assertEquals(1, longRunning.size());
      assertTrue(longRunning.get(0) == bc);
      
      // wait for task to finish now
      blockTillSchedulerIdle(scheduler, 1);

      assertEquals(0, scheduler.getQtyRunningOverTime(0));
      longRunning = scheduler.getCallablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  private class PrioritySchedulerStatisticTrackerTestFactory implements PrioritySchedulerFactory {
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
  
  private static class BlockCallable extends TestCondition implements Callable<Object> {
    private volatile boolean unblock = false;
    private volatile boolean started = false;
    
    public void unblock() {
      unblock = true;
    }
    
    public void blockTillStarted() {
      this.blockTillTrue();
    }

    @Override
    public Object call() {
      started = true;
      
      while (! unblock) {
        TestUtils.sleep(10);
      }
      
      return new Object();
    }

    @Override
    public boolean get() {
      return started;
    }
  }
}
