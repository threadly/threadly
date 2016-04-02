package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class ExecutorQueueLimitRejectorTest extends SubmitterExecutorInterfaceTest {
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorQueueRejectorFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ExecutorQueueLimitRejector(null, TEST_QTY);
  }
  
  @Test
  public void getQueuedTaskCountTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    ExecutorQueueLimitRejector queueRejector = new ExecutorQueueLimitRejector(testableScheduler, TEST_QTY);

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
    ExecutorQueueLimitRejector queueRejector = new ExecutorQueueLimitRejector(testableScheduler, TEST_QTY);
    
    assertEquals(TEST_QTY, queueRejector.getQueueLimit());
    
    queueRejector.setQueueLimit(TEST_QTY * 2);
    assertEquals(TEST_QTY * 2, queueRejector.getQueueLimit());
  }
  
  @Test
  public void rejectTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    ExecutorQueueLimitRejector queueRejector = new ExecutorQueueLimitRejector(testableScheduler, TEST_QTY);
    
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
    ExecutorQueueLimitRejector queueRejector = new ExecutorQueueLimitRejector(new Executor() {
      @Override
      public void execute(Runnable command) {
        throw new RejectedExecutionException();
      }
    }, TEST_QTY);
    
    try {
      queueRejector.execute(DoNothingRunnable.instance());
      fail("Exception should have thrown");
    } catch (RejectedExecutionException e) {
      // expected
    }
    
    assertEquals(0, queueRejector.getQueuedTaskCount());
  }
  
  private static class ExecutorQueueRejectorFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();
    
    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize, prestartIfAvailable);
      return new ExecutorQueueLimitRejector(executor, Integer.MAX_VALUE);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
