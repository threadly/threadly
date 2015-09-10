package org.threadly.concurrent.limiter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.threadly.TestConstants.TEST_QTY;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerQueueLimitRejectorTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SubmitterSchedulerQueueRejectorFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SubmitterSchedulerQueueLimitRejector(null, TEST_QTY);
  }
  
  @Test
  public void rejectTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    SubmitterSchedulerQueueLimitRejector queueRejector = new SubmitterSchedulerQueueLimitRejector(testableScheduler, TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      queueRejector.execute(DoNothingRunnable.instance());
    }
    
    try {
      queueRejector.execute(DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (RejectedExecutionException e) {
      // expected
    }
    
    // verify the task was never added
    assertEquals(TEST_QTY, testableScheduler.tick());
    
    // we should be able to add again now
    for (int i = 0; i < TEST_QTY; i++) {
      queueRejector.execute(DoNothingRunnable.instance());
    }
  }
  
  private static class SubmitterSchedulerQueueRejectorFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduler> schedulers = new ArrayList<PriorityScheduler>(2);

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler ps = new PriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        ps.prestartAllThreads();
      }
      schedulers.add(ps);
      
      return new SubmitterSchedulerQueueLimitRejector(ps, Integer.MAX_VALUE);
    }

    @Override
    public void shutdown() {
      for (PriorityScheduler ps : schedulers) {
        ps.shutdownNow();
      }
    }
  }
}
