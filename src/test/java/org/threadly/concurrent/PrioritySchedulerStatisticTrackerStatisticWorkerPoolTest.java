package org.threadly.concurrent;

import org.junit.Before;
import org.threadly.concurrent.PriorityScheduler.QueueManager;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatisticWorkerPool;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker.StatsManager;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerStatisticWorkerPoolTest extends PrioritySchedulerWorkerPoolTest {
  @Override
  @Before
  public void setup() {
    workerPool = new StatisticWorkerPool(new ConfigurableThreadFactory(), 1, new StatsManager());
    qm = new QueueManager(workerPool, 1000);
    
    // set the queue manager, but then make sure we kill the worker
    workerPool.start(qm);
  }
}
