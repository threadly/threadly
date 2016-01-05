package org.threadly.concurrent.statistics;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ExecutorStatisticWrapperTest extends SubmitterExecutorInterfaceTest {
  private ExecutorStatisticWrapper statWrapper;
  
  @Before
  public void setup() {
    statWrapper = new ExecutorStatisticWrapper(SameThreadSubmitterExecutor.instance());
  }
  
  @After
  public void cleanup() {
    statWrapper = null;
  }
  
  @Override
  protected ExecutorStatisticWrapperFactory getSubmitterExecutorFactory() {
    return new ExecutorStatisticWrapperFactory();
  }
  
  @Test
  public void getExecutionDelaySamplesTest() {
    assertTrue(statWrapper.getExecutionDelaySamples().isEmpty());
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getExecutionDelaySamples().size());
    assertTrue(statWrapper.getExecutionDelaySamples().get(0) < 2);
  }

  @Test
  public void getAverageExecutionDelayTest() {
    assertEquals(0, statWrapper.getAverageExecutionDelay(), 0);
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getAverageExecutionDelay(), 1);
  }
  
  @Test
  public void getExecutionDelayPercentilesTest() {
    assertEquals(0, statWrapper.getExecutionDelayPercentiles(90).get(90.), 0);
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getExecutionDelayPercentiles(90).get(90.), 1);
  }
  
  @Test
  public void getExecutionDurationSamplesTest() {
    assertTrue(statWrapper.getExecutionDurationSamples().isEmpty());
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getExecutionDurationSamples().size());
    statWrapper.execute(new ClockUpdateRunnable(DELAY_TIME));
    assertEquals(2, statWrapper.getExecutionDurationSamples().size());
    
    assertTrue(statWrapper.getExecutionDurationSamples().get(0) < 2);
    assertTrue(statWrapper.getExecutionDurationSamples().get(1) >= DELAY_TIME);
  }

  @Test
  public void getAverageExecutionDurationTest() {
    assertEquals(0, statWrapper.getAverageExecutionDuration(), 0);
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getAverageExecutionDuration(), 1);
    statWrapper.execute(new ClockUpdateRunnable(DELAY_TIME));
    assertTrue(statWrapper.getAverageExecutionDuration() >= DELAY_TIME / 2);
  }

  @Test
  public void getExecutionDurationPercentilesTest() {
    assertEquals(0, statWrapper.getExecutionDurationPercentiles(50).get(50.), 0);
    statWrapper.execute(new ClockUpdateRunnable());
    assertEquals(1, statWrapper.getExecutionDurationPercentiles(50).get(50.), 1);
    statWrapper.execute(new ClockUpdateRunnable());
    statWrapper.execute(new ClockUpdateRunnable());
    statWrapper.execute(new ClockUpdateRunnable());
    statWrapper.execute(new ClockUpdateRunnable(DELAY_TIME));
    assertEquals(1, statWrapper.getExecutionDurationPercentiles(75).get(75.), 1);
    assertTrue(statWrapper.getExecutionDurationPercentiles(90).get(90.) >= DELAY_TIME);
  }
  
  @Test
  public void getLongRunningTasksTest() {
    assertTrue(statWrapper.getLongRunningTasks(-1).isEmpty());
    statWrapper.execute(new ClockUpdateRunnable() {
      @Override
      public void handleRunStart() {
        assertEquals(1, statWrapper.getLongRunningTasks(-1).size());
        assertTrue(statWrapper.getLongRunningTasks(10).isEmpty());
      }
    });
  }
  
  @Test
  public void getLongRunningTasksQtyTest() {
    assertEquals(0, statWrapper.getLongRunningTasksQty(-1));
    statWrapper.execute(new ClockUpdateRunnable() {
      @Override
      public void handleRunStart() {
        assertEquals(1, statWrapper.getLongRunningTasksQty(-1));
        assertEquals(0, statWrapper.getLongRunningTasksQty(10));
      }
    });
  }
  
  private static class ExecutorStatisticWrapperFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);
      
      return new ExecutorStatisticWrapper(ps);
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = schedulers.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
  
  // this class is used because it would be most likely to expose a case where a longer delay than actual is reported
  private static class ClockUpdateRunnable extends TestRunnable {
    public ClockUpdateRunnable() {
      super();
    }
    
    public ClockUpdateRunnable(int runTime) {
      super(runTime);
    }
    
    @Override
    public void handleRunFinish() {
      Clock.accurateTimeNanos();
    }
  }
}
