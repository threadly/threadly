package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;

import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.concurrent.AbstractSubmitterScheduler;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerQueueLimitRejectorTest extends SubmitterSchedulerInterfaceTest {
  @Override
  protected SubmitterSchedulerFactory getSubmitterSchedulerFactory() {
    return new SubmitterSchedulerQueueRejectorFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return true;  // not single threaded, but limit might cause execution delay
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new SubmitterSchedulerQueueLimitRejector(null, TEST_QTY);
  }
  
  @Test
  public void getQueuedTaskCountTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    SchedulerServiceQueueLimitRejector queueRejector = 
        new SchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);

    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(i, queueRejector.getQueuedTaskCount());
      queueRejector.execute(DoNothingRunnable.instance());
    }
    
    testableScheduler.tick();

    assertEquals(0, queueRejector.getQueuedTaskCount());
  }
  
  @Test
  public void getSetQueueLimitTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    SubmitterSchedulerQueueLimitRejector queueRejector = 
        new SubmitterSchedulerQueueLimitRejector(testableScheduler, TEST_QTY);
    
    assertEquals(TEST_QTY, queueRejector.getQueueLimit());
    
    queueRejector.setQueueLimit(TEST_QTY * 2);
    assertEquals(TEST_QTY * 2, queueRejector.getQueueLimit());
  }
  
  @Test
  public void rejectTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    SubmitterSchedulerQueueLimitRejector queueRejector = 
        new SubmitterSchedulerQueueLimitRejector(testableScheduler, TEST_QTY);
    
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
  
  @Test
  public void rejectedExecutionExceptionCountTest() {
    SubmitterSchedulerQueueLimitRejector queueRejector = 
        new SubmitterSchedulerQueueLimitRejector(new AbstractSubmitterScheduler() {
      @Override
      public void scheduleWithFixedDelay(Runnable task, long initialDelay, long recurringDelay) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void scheduleAtFixedRate(Runnable task, long initialDelay, long period) {
        throw new UnsupportedOperationException();
      }

      @Override
      protected void doSchedule(Runnable task, long delayInMillis) {
        throw new RejectedExecutionException();
      }
    }, TEST_QTY);
    
    try {
      queueRejector.schedule(DoNothingRunnable.instance(), 1000);
      fail("Exception should have thrown");
    } catch (RejectedExecutionException e) {
      // expected
    }
    
    assertEquals(0, queueRejector.getQueuedTaskCount());
  }
  
  private static class SubmitterSchedulerQueueRejectorFactory implements SubmitterSchedulerFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      SubmitterScheduler scheduler = schedulerFactory.makeSubmitterScheduler(poolSize, prestartIfAvailable);
      
      return new SubmitterSchedulerQueueLimitRejector(scheduler, Integer.MAX_VALUE);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
