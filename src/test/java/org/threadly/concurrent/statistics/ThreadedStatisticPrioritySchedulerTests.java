package org.threadly.concurrent.statistics;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.TaskPriority;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Pair;

@SuppressWarnings("javadoc")
public class ThreadedStatisticPrioritySchedulerTests extends ThreadlyTester {
  public static void blockTillSchedulerIdle(final StatisticPriorityScheduler scheduler, 
                                            final int expectedSampleSize) {
    new TestCondition() { // block till all are finished
      @Override
      public boolean get() {
        return scheduler.getActiveTaskCount() == 0 && 
                 scheduler.getExecutionDurationSamples().size() >= expectedSampleSize;
      }
    }.blockTillTrue();
  }
  
  public static void resetCollectedStatsTest(StatisticPriorityScheduler scheduler) {
    TestRunnable lastRunnable = null;
    boolean flip = false;
    for (int i = 0; i < TEST_QTY; i++) {
      lastRunnable = new TestRunnable();
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
  }
  
  public static void getTotalExecutionCountTest(StatisticPriorityScheduler scheduler) {
    final int starvablePriorityCount = 1;
    final int lowPriorityCount = TEST_QTY;
    final int highPriorityCount = TEST_QTY * 2;
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
    assertEquals(starvablePriorityCount + lowPriorityCount + highPriorityCount, 
                 scheduler.getTotalExecutionCount());
  }
  
  public static void getAverageExecutionDelayNoInputTest(StatisticPriorityScheduler scheduler) {
    assertEquals(-1, scheduler.getAverageExecutionDelay(), 0);
  }

  public static void getPriorityAverageExecutionDelayNoInputTest(StatisticPriorityScheduler scheduler, 
                                                                 TaskPriority testPriority) {
    assertEquals(-1, scheduler.getAverageExecutionDelay(testPriority), 0);
    
    BlockingTestRunnable br = new BlockingTestRunnable();
    try {
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
      br.unblock();
    }
  }

  public static void getPriorityAverageExecutionDelayTest(StatisticPriorityScheduler scheduler, 
                                                          TaskPriority priority) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
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
    List<Long> allSamples = scheduler.getExecutionDelaySamples();
    if (priority != null) {
      assertTrue(allSamples.size() > samples.size());
    }
    assertTrue(allSamples.containsAll(samples));
    
    double total = 0;
    Iterator<Long> it = samples.iterator();
    while (it.hasNext()) {
      total += it.next();
    }
    
    double expectedAvg = total / samples.size();
    
    assertEquals(expectedAvg, scheduler.getAverageExecutionDelay(priority), 0);
  }
  
  public static void getExecutionDelayPercentilesTest(StatisticPriorityScheduler scheduler) {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
      scheduler.execute(btr);
      scheduler.execute(new ClockUpdateRunnable());
      TestUtils.blockTillClockAdvances();
      btr.unblock();
      assertEquals(2, scheduler.getExecutionDelayPercentiles(90).get(90.), 10);
    } finally {
      btr.unblock();
    }
  }
  
  public static void getPriorityExecutionDelayPercentilesTest(StatisticPriorityScheduler scheduler, 
                                                              TaskPriority priority) {
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
      scheduler.execute(btr);
      scheduler.execute(new ClockUpdateRunnable(), priority);
      TestUtils.blockTillClockAdvances();
      btr.unblock();
      assertEquals(2, scheduler.getExecutionDelayPercentiles(90).get(90.), 10);
    } finally {
      btr.unblock();
    }
  }
  
  public static void getAverageExecutionDurationNoInputTest(StatisticPriorityScheduler scheduler) {
    assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
  }

  public static void getAverageExecutionDurationSimpleTest(StatisticPriorityScheduler scheduler) {
    assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
    scheduler.execute(new ClockUpdateRunnable());
    blockTillSchedulerIdle(scheduler, 1);
    assertEquals(1, scheduler.getAverageExecutionDuration(), 1);
    scheduler.execute(new ClockUpdateRunnable(DELAY_TIME));
    blockTillSchedulerIdle(scheduler, 2);
    assertTrue(scheduler.getAverageExecutionDuration() >= DELAY_TIME / 2);
  }
  
  public static void getAverageExecutionDurationTest(StatisticPriorityScheduler scheduler) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
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
  }
  
  public static void getWithPriorityAverageExecutionDurationNoInputTest(StatisticPriorityScheduler scheduler) {
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr, TaskPriority.Low);
    tr.blockTillFinished();
    blockTillSchedulerIdle(scheduler, 1);
    assertEquals(-1, scheduler.getAverageExecutionDuration(TaskPriority.High), 0);
  }
  
  public static void getWithPriorityAverageExecutionDurationTest(StatisticPriorityScheduler scheduler) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
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
  }

  public static void getExecutionDurationPercentilesTest(StatisticPriorityScheduler scheduler) {
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
    assertTrue(scheduler.getExecutionDurationPercentiles(90).get(90.) >= DELAY_TIME-ALLOWED_VARIANCE);
  }

  public static void getWithPriorityExecutionDurationPercentilesTest(StatisticPriorityScheduler scheduler) {
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
    assertTrue(scheduler.getExecutionDurationPercentiles(TaskPriority.High, 90).get(90.) >= (DELAY_TIME-ALLOWED_VARIANCE));
    assertEquals(1, scheduler.getExecutionDurationPercentiles(TaskPriority.Low, 90).get(90.), 1);
  }
  
  public static void getLongRunningTasksTest(StatisticPriorityScheduler scheduler) {
    final int checkTime = DELAY_TIME;
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
  }
  
  public static void getLongRunningTasksWrappedFutureTest(final StatisticPriorityScheduler scheduler) throws InterruptedException, TimeoutException {
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
  }
  
  public static void getLongRunningTasksQtyTest(final StatisticPriorityScheduler scheduler) {
    assertEquals(0, scheduler.getLongRunningTasksQty(-1));
    scheduler.execute(new ClockUpdateRunnable() {
      @Override
      public void handleRunStart() {
        assertEquals(1, scheduler.getLongRunningTasksQty(-1));
        assertEquals(0, scheduler.getLongRunningTasksQty(10));
      }
    });
  }
}
