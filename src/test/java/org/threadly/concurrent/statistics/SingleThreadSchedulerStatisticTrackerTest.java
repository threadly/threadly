package org.threadly.concurrent.statistics;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.AbstractPriorityScheduler;
import org.threadly.concurrent.ConfigurableThreadFactory;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SingleThreadSchedulerTest;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;

@SuppressWarnings("javadoc")
public class SingleThreadSchedulerStatisticTrackerTest extends SingleThreadSchedulerTest {
  @Override
  protected AbstractPrioritySchedulerFactory getAbstractPrioritySchedulerFactory() {
    return new SingleThreadSchedulerStatisticTrackerTestFactory();
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorTest() {
    new SingleThreadSchedulerStatisticTracker();
    new SingleThreadSchedulerStatisticTracker(false);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, false);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, 
                                              new ConfigurableThreadFactory());
    new SingleThreadSchedulerStatisticTracker(100);
    new SingleThreadSchedulerStatisticTracker(false, 100);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, 100);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, false, 100);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, 
                                              new ConfigurableThreadFactory(), 100);
    new SingleThreadSchedulerStatisticTracker(100, true);
    new SingleThreadSchedulerStatisticTracker(false, 100, true);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, 100, true);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, false, 100, true);
    new SingleThreadSchedulerStatisticTracker(TaskPriority.High, 100, 
                                              new ConfigurableThreadFactory(), 100, true);
  }
  
  @Override
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new SingleThreadSchedulerStatisticTracker(TaskPriority.High, -1, null);
      });
  }
  
  // tests for statistics tracking
  
  @Test
  public void resetCollectedStatsTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      // prestart so reuse percent is not zero
      scheduler.prestartExecutionThread(true);
      
      ThreadedStatisticPrioritySchedulerTests.resetCollectedStatsTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    final SingleThreadSchedulerStatisticTracker scheduler;
    scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getTotalExecutionCountTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDelayNoInputTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayNoInputTest(scheduler, 
                                                                                          TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayNoInputTest(scheduler, 
                                                                                          TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityAverageExecutionDelayTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, null);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, 
                                                                                   TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityAverageExecutionDelayTest(scheduler, 
                                                                                   TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getExecutionDelayPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getExecutionDelayPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getPriorityExecutionDelayPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, null);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityExecutionDelayPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, 
                                                                                       TaskPriority.High);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLowPriorityExecutionDelayPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getPriorityExecutionDelayPercentilesTest(scheduler, 
                                                                                       TaskPriority.Low);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDurationNoInputTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getAverageExecutionDurationSimpleTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationSimpleTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAverageExecutionDurationTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getAverageExecutionDurationTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationNoInputTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getWithPriorityAverageExecutionDurationTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityAverageExecutionDurationNoInputTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getExecutionDurationPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getExecutionDurationPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }

  @Test
  public void getWithPriorityExecutionDurationPercentilesTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getWithPriorityExecutionDurationPercentilesTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker(100, true);
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksWrappedFutureTest() throws InterruptedException, TimeoutException {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksWrappedFutureTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getLongRunningTasksQtyTest() {
    SingleThreadSchedulerStatisticTracker scheduler = new SingleThreadSchedulerStatisticTracker();
    try {
      ThreadedStatisticPrioritySchedulerTests.getLongRunningTasksQtyTest(scheduler);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  private class SingleThreadSchedulerStatisticTrackerTestFactory implements AbstractPrioritySchedulerFactory {
    private final List<PriorityScheduler> executors;
    
    private SingleThreadSchedulerStatisticTrackerTestFactory() {
      executors = new ArrayList<>(1);
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
      SingleThreadSchedulerStatisticTracker result = makeAbstractPriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        result.prestartExecutionThread(true);
      }
      
      return result;
    }

    @Override
    public AbstractPriorityScheduler makeAbstractPriorityScheduler(int poolSize,
                                                                   TaskPriority defaultPriority,
                                                                   long maxWaitForLowPriority) {
      return new SingleThreadSchedulerStatisticTracker(defaultPriority, maxWaitForLowPriority);
    }

    @Override
    public SingleThreadSchedulerStatisticTracker makeAbstractPriorityScheduler(int poolSize) {
      return new SingleThreadSchedulerStatisticTracker();
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
