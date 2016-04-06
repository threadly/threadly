package org.threadly.concurrent.statistics;

import org.junit.Before;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerWorkerPoolTest;
import org.threadly.concurrent.statistics.PrioritySchedulerStatisticTracker.StatisticWorkerPool;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerStatisticWorkerPoolTest extends PrioritySchedulerWorkerPoolTest {
  // needed because of visibility issues with protected inner class
  private StatisticWorkerPool localWorkerPool;
  
  @Before
  @Override
  public void setup() {
    workerPool = localWorkerPool = new StatisticWorkerPool(new ConfigurableThreadFactory(), 1, 
                                                           new PriorityStatisticManager(100, false));
    qm = new VisibilityPriorityScheduler.VisibilityQueueManager(workerPool, 1000);
    
    // set the queue manager, but then make sure we kill the worker
    localWorkerPool.start(qm);
  }
  
  //needed because of visibility issues with protected inner class
  private static class VisibilityPriorityScheduler extends PriorityScheduler {
    public VisibilityPriorityScheduler(int poolSize) {
      super(poolSize);
    }
    
    private static class VisibilityQueueManager extends QueueManager {
      public VisibilityQueueManager(QueueSetListener queueSetListener, 
                                    long maxWaitForLowPriorityInMs) {
        super(queueSetListener, maxWaitForLowPriorityInMs);
      }
    }
  }
}
