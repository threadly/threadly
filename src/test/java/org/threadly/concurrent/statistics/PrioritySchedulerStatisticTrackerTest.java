package org.threadly.concurrent.statistics;

import static org.junit.Assert.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest extends PrioritySchedulerTest {
  @Override
  protected PrioritySchedulerServiceFactory getPrioritySchedulerFactory() {
    return new PrioritySchedulerStatisticTrackerTestFactory();
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorTest() {
    new PrioritySchedulerStatisticTracker(1);
    new PrioritySchedulerStatisticTracker(1, false);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, false);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 
                                          new ConfigurableThreadFactory());
    new PrioritySchedulerStatisticTracker(1, 100);
    new PrioritySchedulerStatisticTracker(1, false, 100);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 100);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, false, 100);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 
                                          new ConfigurableThreadFactory(), 100);
    new PrioritySchedulerStatisticTracker(1, 100, true);
    new PrioritySchedulerStatisticTracker(1, false, 100, true);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 100, true);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, false, 100, true);
    new PrioritySchedulerStatisticTracker(1, TaskPriority.High, 100, 
                                          new ConfigurableThreadFactory(), 100, true);
  }
  
  @Test
  @Override
  public void constructorNullFactoryTest() {
    // ignored due to workerPool visibility
  }
  
  @Test
  @Override
  @SuppressWarnings("unused")
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
  
  // tests for statistics tracking
  
  @Test
  public void resetCollectedStatsTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      // prestart so reuse percent is not zero
      scheduler.prestartAllThreads();
      
      ThreadedStatisticPrioritySchedulerTests.resetCollectedStatsTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    final PrioritySchedulerStatisticTracker scheduler;
    scheduler = new PrioritySchedulerStatisticTracker(10);
    try {
      ThreadedStatisticPrioritySchedulerTests.getTotalExecutionCountTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayNoInputTest(scheduler, 
                                                                                          TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayNoInputTest(scheduler, 
                                                                                          TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityAverageExecutionDelayTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, null);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, 
                                                                                   TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, 
                                                                                   TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getExecutionDelayPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getExecutionDelayPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityExecutionDelayPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, null);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityExecutionDelayPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, 
                                                                                       TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityExecutionDelayPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, 
                                                                                       TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDurationNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getAverageExecutionDurationSimpleTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationSimpleTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDurationTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(10);
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(10);
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityAverageExecutionDurationTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getExecutionDurationPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      ThreadedStatisticPrioritySchedulerTests.getExecutionDurationPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getWithPriorityExecutionDurationPercentilesTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2);
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityExecutionDurationPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 100, true);
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksWrappedFutureTest() throws InterruptedException, TimeoutException {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksWrappedFutureTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksQtyTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1);
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksQtyTest(scheduler);
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
