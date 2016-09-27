package org.threadly.concurrent.wrapper.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.concurrent.RejectedExecutionException;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class PrioritySchedulerServiceQueueLimitRejectorTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PrioritySchedulerServiceQueueRejectorFactory();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new PrioritySchedulerServiceQueueLimitRejector(null, TEST_QTY);
  }
  
  @Test
  public void getSetQueueLimitTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
    assertEquals(TEST_QTY, queueRejector.getQueueLimit());
    
    queueRejector.setQueueLimit(TEST_QTY * 2);
    assertEquals(TEST_QTY * 2, queueRejector.getQueueLimit());
  }
  
  @Test
  public void getQueuedTaskCountTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);

    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(i, queueRejector.getQueuedTaskCount());
      queueRejector.execute(DoNothingRunnable.instance());
    }

    assertEquals(TEST_QTY, testableScheduler.tick());

    assertEquals(0, queueRejector.getQueuedTaskCount());
  }
  
  @Test
  public void getQueuedTaskByPriorityCountTest() {
    getQueuedTaskByPriorityCountTest(TaskPriority.High);
    getQueuedTaskByPriorityCountTest(TaskPriority.Low);
    getQueuedTaskByPriorityCountTest(TaskPriority.Starvable);
  }

  private static void getQueuedTaskByPriorityCountTest(TaskPriority submitPriority) {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);

    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(TaskPriority.High == submitPriority ? i : 0, queueRejector.getQueuedTaskCount(TaskPriority.High));
      assertEquals(TaskPriority.Low == submitPriority ? i : 0, queueRejector.getQueuedTaskCount(TaskPriority.Low));
      assertEquals(TaskPriority.Starvable == submitPriority ? i : 0, queueRejector.getQueuedTaskCount(TaskPriority.Starvable));
      queueRejector.execute(DoNothingRunnable.instance(), submitPriority);
    }

    assertEquals(TEST_QTY, testableScheduler.tick());

    assertEquals(0, queueRejector.getQueuedTaskCount(submitPriority));
  }
  
  @Test
  public void rejectRunnableTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
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
  public void rejectCallableTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      queueRejector.submit(new TestCallable());
    }
    
    try {
      queueRejector.submit(new TestCallable());
      fail("Exception should have thrown");
    } catch (RejectedExecutionException e) {
      // expected
    }
    
    // verify the task was never added
    assertEquals(TEST_QTY, testableScheduler.tick());
    
    // we should be able to add again now
    for (int i = 0; i < TEST_QTY; i++) {
      queueRejector.submit(new TestCallable());
    }
  }
  
  @Test
  public void starvablePriorityNotIgnoredTest() {
    starvablePriorityIgnoredTest(false);
  }
  
  @Test
  public void starvablePriorityIgnoredTest() {
    starvablePriorityIgnoredTest(true);
  }
  
  private static void starvablePriorityIgnoredTest(boolean ignored) {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY, ignored);

    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(ignored ? 0 : i, queueRejector.getQueuedTaskCount());
      queueRejector.execute(DoNothingRunnable.instance(), TaskPriority.Starvable);
    }
    
    assertEquals(TEST_QTY, testableScheduler.tick());
  }
  
  private static class PrioritySchedulerServiceQueueRejectorFactory implements SchedulerServiceFactory {
    private final PrioritySchedulerFactory schedulerFactory = new PrioritySchedulerFactory();

    @Override
    public SubmitterExecutor makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterScheduler makeSubmitterScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerService makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler scheduler = schedulerFactory.makePriorityScheduler(poolSize);
      if (prestartIfAvailable) {
        scheduler.prestartAllThreads();
      }
      
      return new PrioritySchedulerServiceQueueLimitRejector(scheduler, Integer.MAX_VALUE);
    }

    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
