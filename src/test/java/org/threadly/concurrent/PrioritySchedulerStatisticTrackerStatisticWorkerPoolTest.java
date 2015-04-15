package org.threadly.concurrent;

import org.junit.Before;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatisticWorkerPool;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatsManager;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerStatisticWorkerPoolTest extends PrioritySchedulerWorkerPoolTest {
  @Before
  public void setup() {
    workerPool = new StatisticWorkerPool(new ConfigurableThreadFactory(), 1, new StatsManager());
  }
}
