package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest;
import org.threadly.concurrent.StrictPriorityScheduledExecutor;
import org.threadly.concurrent.SubmitterExecutorInterface;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterSchedulerInterface;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class PrioritySchedulerLimiterTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Test
  public void constructorFail() {
    try {
      new PrioritySchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      new PrioritySchedulerLimiter(executor, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void getDefaultPriorityTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 10, TaskPriority.Low, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
    
    executor = new StrictPriorityScheduledExecutor(1, 1, 10, TaskPriority.High, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
  }
  
  @Test
  public void consumeAvailableTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 10, TaskPriority.High, 100);
    try {
      PrioritySchedulerLimiter psl = new PrioritySchedulerLimiter(executor, TEST_QTY);
      
      boolean flip = true;
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        runnables.add(tr);
        if (flip) {
          psl.waitingTasks.add(psl.new PriorityWrapper(tr, TaskPriority.High));
          flip = false;
        } else {
          psl.waitingTasks.add(psl.new PriorityWrapper(tr, TaskPriority.High));
          flip = true;
        }
      }
      
      psl.consumeAvailable();
      
      // should be fully consumed
      assertEquals(0, psl.waitingTasks.size());
      
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        it.next().blockTillFinished();  // throws exception if it does not finish
      }
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void executeLimitTest() throws InterruptedException, TimeoutException {
    final int limiterLimit = TEST_QTY / 2;
    final int threadCount = limiterLimit * 2;
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(threadCount, threadCount, 10, 
                                                                             TaskPriority.High, 100);
    try {
      PrioritySchedulerLimiter psl = new PrioritySchedulerLimiter(executor, limiterLimit);
      
      ExecutorLimiterTest.executeLimitTest(psl, limiterLimit);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void executeTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void executeNamedSubPoolTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void submitRunnableTest() {
    submitRunnableTest(false);
  }
  
  @Test
  public void submitRunnableNamedSubPoolTest() {
    submitRunnableTest(true);
  }
  
  public void submitRunnableTest(boolean nameSubPool) {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(nameSubPool);
    
    try {
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<Future<?>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submit(tr);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
      
      Iterator<Future<?>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> f = futureIt.next();
        try {
          f.get();
        } catch (InterruptedException e) {
          fail();
        } catch (ExecutionException e) {
          fail();
        }
        assertTrue(f.isDone());
      }
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test
  public void submitCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionNamedSubPoolTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.scheduleFail(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    submitScheduledRunnableTest(false);
  }
  
  @Test
  public void submitScheduledRunnableNamedSubPoolTest() {
    submitScheduledRunnableTest(true);
  }
  
  public void submitScheduledRunnableTest(boolean nameSubPool) {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(nameSubPool);
    // we can't defer to the interface implementation for this check
    try {
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(TEST_QTY, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<Future<?>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submitScheduled(tr, SCHEDULE_DELAY);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= SCHEDULE_DELAY);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (SCHEDULE_DELAY + 2000));  
        assertEquals(1, tr.getRunCount());
      }
      
      Iterator<Future<?>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> f = futureIt.next();
        try {
          f.get();
        } catch (InterruptedException e) {
          fail();
        } catch (ExecutionException e) {
          fail();
        }
        assertTrue(f.isDone());
      }
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableNamedSubPoolTest() throws InterruptedException, 
                                                               ExecutionException, 
                                                               TimeoutException {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void recurringExecutionTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionNamedSubPoolTest() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    PrioritySchedulerLimiterFactory sf = new PrioritySchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }
  
  @Test
  public void removeHighPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.Low);
  }
  
  public static void removeRunnableTest(TaskPriority priority) {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(2, 2, 1000);
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(scheduler, 2);
      
      TestRunnable task = new TestRunnable();
      limiter.schedule(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeHighPriorityCallableTest() {
    removeCallableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityCallableTest() {
    removeCallableTest(TaskPriority.Low);
  }
  
  public static void removeCallableTest(TaskPriority priority) {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(2, 2, 1000);
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(scheduler, 1);
      
      TestCallable task = new TestCallable();
      limiter.submitScheduled(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeHighPriorityBlockedRunnableTest() {
    removeBlockedRunnableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityBlockedRunnableTest() {
    removeBlockedRunnableTest(TaskPriority.Low);
  }
  
  public static void removeBlockedRunnableTest(TaskPriority priority) {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable, priority);
      scheduler.execute(blockingRunnable, priority);
      blockingRunnable.blockTillStarted();
      
      TestRunnable task = new TestRunnable();
      limiter.execute(task, priority);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      assertTrue(scheduler.remove(task));
    } finally {
      blockingRunnable.unblock();
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeHighPriorityBlockedCallableTest() {
    removeBlockedCallableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityBlockedCallableTest() {
    removeBlockedCallableTest(TaskPriority.Low);
  }
  
  public static void removeBlockedCallableTest(TaskPriority priority) {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable, priority);
      scheduler.execute(blockingRunnable, priority);
      blockingRunnable.blockTillStarted();

      TestCallable task = new TestCallable();
      limiter.submit(task, priority);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      blockingRunnable.unblock();
      scheduler.shutdownNow();
    }
  }

  private class PrioritySchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    private final boolean addSubPoolName;
    
    private PrioritySchedulerLimiterFactory(boolean addSubPoolName) {
      executors = new LinkedList<PriorityScheduledExecutor>();
      this.addSubPoolName = addSubPoolName;
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, 
                                                        boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public PrioritySchedulerLimiter makeSubmitterScheduler(int poolSize, 
                                                           boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                               1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      if (addSubPoolName) {
        return new PrioritySchedulerLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new PrioritySchedulerLimiter(executor, poolSize);
      }
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
}
