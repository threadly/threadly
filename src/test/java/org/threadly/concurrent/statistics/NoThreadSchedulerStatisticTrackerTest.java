package org.threadly.concurrent.statistics;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.NoThreadSchedulerTest;
import org.threadly.concurrent.TaskPriority;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerStatisticTrackerTest extends NoThreadSchedulerTest {
  @Before
  @Override
  public void setup() {
    scheduler = new NoThreadSchedulerStatisticTracker();
  }
  
  // tests for statistics tracking
  
  @Test
  public void resetCollectedStatsTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
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
    
    scheduler.tick(null);
    
    // reset stats
    scheduler.resetCollectedStats();
    
    assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
    for (TaskPriority p : TaskPriority.values()) {
      assertEquals(-1, scheduler.getAverageExecutionDelay(p), 0);
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    final int starvablePriorityCount = 1;
    final int lowPriorityCount = TEST_QTY;
    final int highPriorityCount = TEST_QTY * 2;
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    for (int i = 0; i < starvablePriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Starvable);
    }
    for (int i = 0; i < lowPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    }
    for (int i = 0; i < highPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
    }
    
    // should not be very long after waiting above
    scheduler.tick(null);
    
    assertEquals(starvablePriorityCount, scheduler.getTotalExecutionCount(TaskPriority.Starvable));
    assertEquals(lowPriorityCount, scheduler.getTotalExecutionCount(TaskPriority.Low));
    assertEquals(highPriorityCount, scheduler.getTotalExecutionCount(TaskPriority.High));
    assertEquals(starvablePriorityCount + lowPriorityCount + highPriorityCount, 
                 scheduler.getTotalExecutionCount(null));
  }
  
  @Test
  public void getAverageExecutionDelayNoInputTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(-1, scheduler.getAverageExecutionDelay(), 0);
  }

  public void getPriorityAverageExecutionDelayNoInputTest(TaskPriority testPriority) {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    switch (testPriority) { // submit with opposite priority
      case High:
        scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
        break;
      case Low:
        scheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
        break;
      default:
        throw new UnsupportedOperationException("Priority not implenented: " + testPriority);
    }
    
    // wait for task to finish now
    scheduler.tick(null);
    
    assertEquals(-1, scheduler.getAverageExecutionDelay(testPriority), 0);
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
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    for (int i = 0; i < lowPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    }
    TestUtils.blockTillClockAdvances();
    scheduler.tick(null);
    for (int i = 0; i < highPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
    }
    TestUtils.blockTillClockAdvances();
    scheduler.tick(null);
    
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
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
    scheduler.execute(new ClockUpdateRunnable());
    TestUtils.blockTillClockAdvances();
    scheduler.tick(null);
    assertEquals(1, scheduler.getExecutionDelayPercentiles(90).get(90.), 2);
  }
  
  protected void getPriorityExecutionDelayPercentilesTest(TaskPriority priority) {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(0, scheduler.getExecutionDelayPercentiles(90).get(90.), 0);
    scheduler.execute(new ClockUpdateRunnable(), priority);
    TestUtils.blockTillClockAdvances();
    scheduler.tick(null);
    assertEquals(1, scheduler.getExecutionDelayPercentiles(90).get(90.), 20);
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
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
  }

  @Test
  public void getAverageExecutionDurationSimpleTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(-1, scheduler.getAverageExecutionDuration(), 0);
    scheduler.execute(new ClockUpdateRunnable());
    scheduler.tick(null);
    assertEquals(1, scheduler.getAverageExecutionDuration(), 1);
    scheduler.execute(new ClockUpdateRunnable(DELAY_TIME));
    scheduler.tick(null);
    assertTrue(scheduler.getAverageExecutionDuration() >= DELAY_TIME / 2);
  }
  
  @Test
  public void getAverageExecutionDurationTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    for (int i = 0; i < lowPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    }
    for (int i = 0; i < highPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
    }
    
    // block till all are finished
    scheduler.tick(null);
    
    List<Long> runTimes = scheduler.getExecutionDurationSamples();
    assertEquals(lowPriorityCount + highPriorityCount, runTimes.size());
    
    double totalRunTime = 0;
    Iterator<Long> it = runTimes.iterator();
    while (it.hasNext()) {
      totalRunTime += it.next();
    }
    
    double avgRunTime = totalRunTime / runTimes.size();
    
    assertEquals(avgRunTime, scheduler.getAverageExecutionDuration(), 0);
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationNoInputTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    scheduler.tick(null);
    assertEquals(-1, scheduler.getAverageExecutionDuration(TaskPriority.High), 0);
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    for (int i = 0; i < lowPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    }
    for (int i = 0; i < highPriorityCount; i++) {
      scheduler.execute(DoNothingRunnable.instance(), TaskPriority.High);
    }
    
    // block till all are finished
    scheduler.tick(null);
    
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

  @Test
  public void getExecutionDurationPercentilesTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(0, scheduler.getExecutionDurationPercentiles(50).get(50.), 0);
    scheduler.execute(new ClockUpdateRunnable());
    scheduler.tick(null);
    assertEquals(1, scheduler.getExecutionDurationPercentiles(50).get(50.), 1);
    scheduler.execute(new ClockUpdateRunnable());
    scheduler.execute(new ClockUpdateRunnable());
    scheduler.execute(new ClockUpdateRunnable());
    scheduler.execute(new ClockUpdateRunnable(DELAY_TIME));
    scheduler.tick(null);
    assertEquals(1, scheduler.getExecutionDurationPercentiles(75).get(75.), 1);
    assertTrue(scheduler.getExecutionDurationPercentiles(90).get(90.) >= DELAY_TIME);
  }

  @Test
  public void getWithPriorityExecutionDurationPercentilesTest() {
    NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
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
    scheduler.tick(null);
    assertEquals(1, scheduler.getExecutionDurationPercentiles(TaskPriority.High, 75).get(75.), 1);
    assertTrue(scheduler.getExecutionDurationPercentiles(TaskPriority.High, 90).get(90.) >= DELAY_TIME);
    assertEquals(1, scheduler.getExecutionDurationPercentiles(TaskPriority.Low, 90).get(90.), 1);
  }
  
  @Test
  public void getLongRunningTasksWrappedFutureTest() {
    final NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    scheduler.submit(new ClockUpdateRunnable() {
      @Override
      public void handleRunStart() {
        // even submitted (and thus wrapped in a future), we should get our direct reference
        assertTrue(scheduler.getLongRunningTasks(-1).get(0).getLeft() == this);
      }
    });
    scheduler.tick(null);
  }
  
  @Test
  public void getLongRunningTasksQtyTest() {
    final NoThreadSchedulerStatisticTracker scheduler = new NoThreadSchedulerStatisticTracker();
    assertEquals(0, scheduler.getLongRunningTasksQty(-1));
    scheduler.execute(new ClockUpdateRunnable() {
      @Override
      public void handleRunStart() {
        assertEquals(1, scheduler.getLongRunningTasksQty(-1));
        assertEquals(0, scheduler.getLongRunningTasksQty(10));
      }
    });
    scheduler.tick(null);
  }
}
