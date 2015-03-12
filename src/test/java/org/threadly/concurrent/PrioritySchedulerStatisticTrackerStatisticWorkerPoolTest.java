package org.threadly.concurrent;

import org.junit.Before;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatisticWorkerPool;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatsManager;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerStatisticWorkerPoolTest extends PrioritySchedulerWorkerPoolTest {
  @Before
  public void setup() {
    workerPool = new StatisticWorkerPool(new ConfigurableThreadFactory(), 1, 1, DEFAULT_KEEP_ALIVE_TIME, 
                                         PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, new StatsManager());
  }
}
