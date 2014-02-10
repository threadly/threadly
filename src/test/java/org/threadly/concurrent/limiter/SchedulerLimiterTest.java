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
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.concurrent.TaskPriority;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SchedulerLimiterTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @Test
  public void constructorFail() {
    try {
      new SchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      new SchedulerLimiter(executor, 0);
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
      SchedulerLimiter limiter = new SchedulerLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void consumeAvailableTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 10, 
                                                                             TaskPriority.High, 100);
    try {
      SchedulerLimiter limiter = new SchedulerLimiter(executor, TEST_QTY);
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        runnables.add(tr);
        limiter.waitingTasks.add(limiter.new LimiterRunnableWrapper(tr));
      }
      
      limiter.consumeAvailable();
      
      // should be fully consumed
      assertEquals(0, limiter.waitingTasks.size());
      
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
      SchedulerLimiter sl = new SchedulerLimiter(executor, limiterLimit);
      
      ExecutorLimiterTest.executeLimitTest(sl, limiterLimit);
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
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeFail(sf);
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
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test
  public void submitCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
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
  public void submitScheduledRunnableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableNamedSubPoolTest() throws InterruptedException, 
                                                               ExecutionException, 
                                                               TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, 
                                                             ExecutionException, 
                                                             TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableWithResultNamedSubPoolTest() throws InterruptedException, 
                                                                         ExecutionException, 
                                                                         TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableNamedSubPoolTest() throws InterruptedException, 
                                                               ExecutionException, 
                                                               TimeoutException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void removeRunnableTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(2, 2, 1000);
    try {
      SchedulerLimiter limiter = new SchedulerLimiter(scheduler, 2);
      
      TestRunnable task = new TestRunnable();
      limiter.schedule(task, 1000 * 10);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeBlockedRunnableTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      SchedulerLimiter limiter = new SchedulerLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable);
      scheduler.execute(blockingRunnable);
      blockingRunnable.blockTillStarted();
      
      TestRunnable task = new TestRunnable();
      limiter.execute(task);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      assertTrue(scheduler.remove(task));
    } finally {
      blockingRunnable.unblock();
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeCallableTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(2, 2, 1000);
    try {
      SchedulerLimiter limiter = new SchedulerLimiter(scheduler, 1);
      
      TestCallable task = new TestCallable();
      limiter.submitScheduled(task, 1000 * 10);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeBlockedCallableTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      SchedulerLimiter limiter = new SchedulerLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable);
      scheduler.execute(blockingRunnable);
      blockingRunnable.blockTillStarted();

      TestCallable task = new TestCallable();
      limiter.submit(task);
      
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
      executors = new LinkedList<PriorityScheduledExecutor>();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                               1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      if (addSubPoolName) {
        return new SchedulerLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new SchedulerLimiter(executor, poolSize);
      }
    }
  }
}
