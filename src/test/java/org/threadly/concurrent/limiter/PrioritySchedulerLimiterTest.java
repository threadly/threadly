package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.concurrent.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest;
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
  @Test
  public void constructorFail() {
    try {
      new PrioritySchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 100);
    try {
      new PrioritySchedulerLimiter(executor, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 100);
    try {
      PrioritySchedulerLimiter limiter = new PrioritySchedulerLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void getDefaultPriorityTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 10, TaskPriority.Low, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
    
    executor = new PriorityScheduledExecutor(1, 1, 10, TaskPriority.High, 100);
    assertTrue(new PrioritySchedulerLimiter(executor, 1).getDefaultPriority() == executor.getDefaultPriority());
  }
  
  @Test
  public void consumeAvailableTest() {
    int testQty = 10;
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 10, TaskPriority.High, 100);
    PrioritySchedulerLimiter psl = new PrioritySchedulerLimiter(executor, testQty);
    
    boolean flip = true;
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(testQty);
    for (int i = 0; i < testQty; i++) {
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
  }
  
  @Test
  public void executeLimitTest() throws InterruptedException, TimeoutException {
    final int limiterLimit = 2;
    final int threadCount = limiterLimit * 2;
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(threadCount, threadCount, 10, 
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
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void executeNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
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
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(nameSubPool);
    
    try {
      int runnableCount = 10;
      
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
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
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test
  public void submitCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
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
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(nameSubPool);
    // we can't defer to the interface implementation for this check
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(runnableCount, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submitScheduled(tr, scheduleDelay);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= scheduleDelay);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (scheduleDelay + 2000));  
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
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
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

  private class SchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    private final boolean addSubPoolName;
    
    private SchedulerLimiterFactory(boolean addSubPoolName) {
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          // ignored
        }
      });
      
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
      PriorityScheduledExecutor executor = new PriorityScheduledExecutor(poolSize, poolSize, 
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
        it.next().shutdown();
        it.remove();
      }
    }
  }
}
