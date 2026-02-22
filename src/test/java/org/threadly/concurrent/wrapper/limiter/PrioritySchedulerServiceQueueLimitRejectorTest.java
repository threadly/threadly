package org.threadly.concurrent.wrapper.limiter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.RejectedExecutionException;

import org.junit.jupiter.api.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SchedulerService;
import org.threadly.concurrent.SchedulerServiceInterfaceTest;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterScheduler;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class PrioritySchedulerServiceQueueLimitRejectorTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return new PrioritySchedulerServiceQueueRejectorFactory();
  }
  
  @Override
  protected boolean isSingleThreaded() {
    return false;
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      new PrioritySchedulerServiceQueueLimitRejector(null, TEST_QTY);
      });
  }
  
  @Test
  public void basicExecuteTest() {
    TestRunnable tr = new TestRunnable();
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);

    queueRejector.execute(tr);
    queueRejector.execute(tr, TaskPriority.Low);
    queueRejector.submit(tr);
    queueRejector.submit(tr, TaskPriority.Low);
    
    assertEquals(4, testableScheduler.tick());
    assertEquals(4, tr.getRunCount());
  }
  
  @Test
  public void basicScheduleTest() {
    TestRunnable tr = new TestRunnable();
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);

    queueRejector.schedule(tr, DELAY_TIME);
    queueRejector.schedule(tr, DELAY_TIME, TaskPriority.Low);
    queueRejector.submitScheduled(tr, DELAY_TIME);
    queueRejector.submitScheduled(tr, DELAY_TIME, TaskPriority.Low);
    
    assertEquals(4, testableScheduler.advance(DELAY_TIME));
    assertEquals(4, tr.getRunCount());
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
  @Override
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
  
  @Test
  public void getDefaultPriorityTest() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
    
    assertEquals(testableScheduler.getDefaultPriority(), queueRejector.getDefaultPriority());
  }
  
  @Test
  public void getMaxWaitForLowPriority() {
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
    
    assertEquals(testableScheduler.getMaxWaitForLowPriority(), 
                 queueRejector.getMaxWaitForLowPriority());
  }
  
  @Test
  @Override
  public void getWaitingForExecutionTaskCountTest() {
    super.getWaitingForExecutionTaskCountTest();  // more complete tests, we focus here on priorities
    
    TestableScheduler testableScheduler = new TestableScheduler();
    PrioritySchedulerServiceQueueLimitRejector queueRejector = 
        new PrioritySchedulerServiceQueueLimitRejector(testableScheduler, TEST_QTY);
    
    assertEquals(0, queueRejector.getWaitingForExecutionTaskCount());
    
    queueRejector.execute(DoNothingRunnable.instance(), TaskPriority.Low);
    assertEquals(1, queueRejector.getWaitingForExecutionTaskCount());
    assertEquals(0, queueRejector.getWaitingForExecutionTaskCount(TaskPriority.High));
    assertEquals(1, queueRejector.getWaitingForExecutionTaskCount(TaskPriority.Low));
    
    testableScheduler.tick();
    assertEquals(0, queueRejector.getWaitingForExecutionTaskCount());
    assertEquals(0, queueRejector.getWaitingForExecutionTaskCount(TaskPriority.Low));
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
