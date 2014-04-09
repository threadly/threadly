package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.Worker;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorTest {
  private static void ensureIdleWorker(PriorityScheduledExecutor scheduler) {
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr);
    tr.blockTillStarted();
     
    // block till the worker is finished
    blockTillWorkerAvailable(scheduler);
    
    // verify we have a worker
    assertEquals(1, scheduler.getCurrentPoolSize());

    TestUtils.blockTillClockAdvances();
  }
  
  private static void blockTillWorkerAvailable(final PriorityScheduledExecutor scheduler) {
    new TestCondition() {
      @Override
      public boolean get() {
        synchronized (scheduler.workersLock) {
          return ! scheduler.availableWorkers.isEmpty();
        }
      }
    }.blockTillTrue();
  }
  
  @Test
  public void getDefaultPriorityTest() {
    getDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  } 
  
  public static void getDefaultPriorityTest(PriorityScheduledExecutorFactory factory) {
    TaskPriority priority = TaskPriority.High;
    try {
      PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000, 
                                                         priority, 1000);
      
      assertEquals(priority, scheduler.getDefaultPriority());
      
      priority = TaskPriority.Low;
      scheduler = factory.make(1, 1, 1000, 
                               priority, 1000);
      assertEquals(priority, scheduler.getDefaultPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new StrictPriorityScheduledExecutor(0, 1, 1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new StrictPriorityScheduledExecutor(2, 1, 1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new StrictPriorityScheduledExecutor(1, 1, -1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new StrictPriorityScheduledExecutor(1, 1, 1, TaskPriority.High, -1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void constructorNullPriorityTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 1, null, 1, null);
    
    assertTrue(executor.getDefaultPriority() == PriorityScheduledExecutor.DEFAULT_PRIORITY);
  }
  
  @Test
  public void makeWithDefaultPriorityTest() {
    makeWithDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void makeWithDefaultPriorityTest(PriorityScheduledExecutorFactory factory) {
    TaskPriority originalPriority = TaskPriority.Low;
    TaskPriority newPriority = TaskPriority.High;
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000, 
                                                       originalPriority, 1000);
    assertTrue(scheduler.makeWithDefaultPriority(originalPriority) == scheduler);
    PrioritySchedulerInterface newScheduler = scheduler.makeWithDefaultPriority(newPriority);
    try {
      assertEquals(newPriority, newScheduler.getDefaultPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetCorePoolSizeTest() {
    getAndSetCorePoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetCorePoolSizeTest(PriorityScheduledExecutorFactory factory) {
    int corePoolSize = 1;
    PriorityScheduledExecutor scheduler = factory.make(corePoolSize, 
                                                       corePoolSize + 10, 1000);
    try {
      assertEquals(corePoolSize, scheduler.getCorePoolSize());
      
      corePoolSize = 10;
      scheduler.setMaxPoolSize(corePoolSize + 10);
      scheduler.setCorePoolSize(corePoolSize);
      
      assertEquals(corePoolSize, scheduler.getCorePoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetCorePoolSizeAboveMaxTest() {
    getAndSetCorePoolSizeAboveMaxTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetCorePoolSizeAboveMaxTest(PriorityScheduledExecutorFactory factory) {
    int corePoolSize = 1;
    PriorityScheduledExecutor scheduler = factory.make(corePoolSize, 
                                                       corePoolSize, 1000);
    try {
      corePoolSize = scheduler.getMaxPoolSize() * 2;
      scheduler.setCorePoolSize(corePoolSize);
      
      assertEquals(corePoolSize, scheduler.getCorePoolSize());
      assertEquals(corePoolSize, scheduler.getMaxPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void lowerSetCorePoolSizeCleansWorkerTest() {
    lowerSetCorePoolSizeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void lowerSetCorePoolSizeCleansWorkerTest(PriorityScheduledExecutorFactory factory) {
    final int poolSize = 5;
    PriorityScheduledExecutor scheduler = factory.make(poolSize, poolSize, 0); // must have no keep alive time to work
    try {
      ensureIdleWorker(scheduler);
      // must allow core thread timeout for this to work
      scheduler.allowCoreThreadTimeOut(true);
      
      scheduler.setCorePoolSize(1);
      
      // verify worker was cleaned up
      assertEquals(0, scheduler.getCurrentPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setCorePoolSizeFail() {
    setCorePoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }

  public static void setCorePoolSizeFail(PriorityScheduledExecutorFactory factory) {
    int corePoolSize = 1;
    int maxPoolSize = 10;
    // first construct a valid scheduler
    PriorityScheduledExecutor scheduler = factory.make(corePoolSize, 
                                                       maxPoolSize, 1000);
    try {
      // verify no negative values
      try {
        scheduler.setCorePoolSize(-1);
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException expected) {
        // ignored
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    getAndSetMaxPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetMaxPoolSizeTest(PriorityScheduledExecutorFactory factory) {
    final int originalCorePoolSize = 5;
    int maxPoolSize = originalCorePoolSize;
    PriorityScheduledExecutor scheduler = factory.make(originalCorePoolSize, maxPoolSize, 1000);
    try {
      maxPoolSize *= 2;
      scheduler.setMaxPoolSize(maxPoolSize);
      
      assertEquals(maxPoolSize, scheduler.getMaxPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetMaxPoolSizeBelowCoreTest() {
    getAndSetMaxPoolSizeBelowCoreTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetMaxPoolSizeBelowCoreTest(PriorityScheduledExecutorFactory factory) {
    final int originalPoolSize = 5;  // must be above 1
    int maxPoolSize = originalPoolSize;
    PriorityScheduledExecutor scheduler = factory.make(originalPoolSize, maxPoolSize, 1000);
    try {
      maxPoolSize = 1;
      scheduler.setMaxPoolSize(1);
      
      assertEquals(maxPoolSize, scheduler.getMaxPoolSize());
      assertEquals(maxPoolSize, scheduler.getCorePoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void lowerSetMaxPoolSizeCleansWorkerTest() {
    lowerSetMaxPoolSizeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void lowerSetMaxPoolSizeCleansWorkerTest(PriorityScheduledExecutorFactory factory) {
    final int poolSize = 5;
    PriorityScheduledExecutor scheduler = factory.make(poolSize, poolSize, 0); // must have no keep alive time to work
    try {
      ensureIdleWorker(scheduler);
      // must allow core thread timeout for this to work
      scheduler.allowCoreThreadTimeOut(true);
      
      scheduler.setMaxPoolSize(1);
      
      // verify worker was cleaned up
      assertEquals(0, scheduler.getCurrentPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setMaxPoolSizeFail() {
    setMaxPoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void setMaxPoolSizeFail(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(2, 2, 1000);
    
    try {
      try {
        scheduler.setMaxPoolSize(-1); // should throw exception for negative value
        fail("Exception should have been thrown");
      } catch (IllegalArgumentException e) {
        //expected
      }
    } finally {
      factory.shutdown();
    }
  }

  @Test
  public void setMaxPoolSizeBlockedThreadsTest() {
    getDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  } 
  
  public static void setMaxPoolSizeUnblockedThreadTest(PriorityScheduledExecutorFactory factory) {
    try {
      PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
      
      BlockingTestRunnable btr = new BlockingTestRunnable();
      try {
        scheduler.execute(btr);
        
        btr.blockTillStarted();
        
        TestRunnable tr = new TestRunnable();
        scheduler.execute(tr);
        // should not be able to start
        assertEquals(0, tr.getRunCount());
        
        scheduler.setMaxPoolSize(2);
        
        // tr should not be able to start, will throw exception if unable to
        tr.blockTillStarted();
        assertEquals(1, tr.getRunCount());
      } finally {
        btr.unblock();
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    getAndSetLowPriorityWaitTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetLowPriorityWaitTest(PriorityScheduledExecutorFactory factory) {
    long lowPriorityWait = 1000;
    PriorityScheduledExecutor scheduler = factory.make(1, 1, lowPriorityWait / 10, TaskPriority.High, lowPriorityWait);
    try {
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
      
      lowPriorityWait = Long.MAX_VALUE;
      scheduler.setMaxWaitForLowPriority(lowPriorityWait);
      
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    setLowPriorityWaitFail(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void setLowPriorityWaitFail(PriorityScheduledExecutorFactory factory) {
    long lowPriorityWait = 1000;
    PriorityScheduledExecutor scheduler = factory.make(1, 1, lowPriorityWait / 10, TaskPriority.High, lowPriorityWait);
    try {
      try {
        scheduler.setMaxWaitForLowPriority(-1);
        fail("Exception should have thrown");
      } catch (IllegalArgumentException e) {
        // expected
      }
      
      assertEquals(lowPriorityWait, scheduler.getMaxWaitForLowPriority());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getAndSetKeepAliveTimeTest() {
    getAndSetKeepAliveTimeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getAndSetKeepAliveTimeTest(PriorityScheduledExecutorFactory factory) {
    long keepAliveTime = 1000;
    PriorityScheduledExecutor scheduler = factory.make(1, 1, keepAliveTime);
    try {
      assertEquals(keepAliveTime, scheduler.getKeepAliveTime());
      
      keepAliveTime = Long.MAX_VALUE;
      scheduler.setKeepAliveTime(keepAliveTime);
      
      assertEquals(keepAliveTime, scheduler.getKeepAliveTime());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void lowerSetKeepAliveTimeCleansWorkerTest() {
    lowerSetKeepAliveTimeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void lowerSetKeepAliveTimeCleansWorkerTest(PriorityScheduledExecutorFactory factory) {
    long keepAliveTime = 1000;
    final PriorityScheduledExecutor scheduler = factory.make(1, 1, keepAliveTime);
    try {
      ensureIdleWorker(scheduler);
      // must allow core thread timeout for this to work
      scheduler.allowCoreThreadTimeOut(true);
      
      scheduler.setKeepAliveTime(0);
      
      // verify worker was cleaned up
      assertEquals(0, scheduler.getCurrentPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setKeepAliveTimeFail() {
    setKeepAliveTimeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void setKeepAliveTimeFail(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    
    try {
      scheduler.setKeepAliveTime(-1L); // should throw exception for negative value
      fail("Exception should have been thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountTest() {
    PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(1, 1, 1000);
    // add directly to avoid starting the consumer
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    
    assertEquals(2, result.getScheduledTaskCount());
    
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    
    assertEquals(4, result.getScheduledTaskCount());
    assertEquals(4, result.getScheduledTaskCount(null));
  }
  
  @Test
  public void getScheduledTaskCountLowPriorityTest() {
    PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(1, 1, 1000);
    // add directly to avoid starting the consumer
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    
    assertEquals(0, result.getScheduledTaskCount(TaskPriority.Low));
    
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    
    assertEquals(2, result.getScheduledTaskCount(TaskPriority.Low));
  }
  
  @Test
  public void getScheduledTaskCountHighPriorityTest() {
    PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(1, 1, 1000);
    // add directly to avoid starting the consumer
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    result.highPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                        TaskPriority.High, 0));
    
    assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
    
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    result.lowPriorityQueue.add(new OneTimeTaskWrapper(new TestRunnable(), 
                                                      TaskPriority.Low, 0));
    
    assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    getCurrentPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getCurrentPoolSizeTest(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getCurrentPoolSize());
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      
      tr.blockTillStarted();  // wait for execution
      
      assertEquals(1, scheduler.getCurrentPoolSize());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void makeSubPoolTest() {
    makeSubPoolTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void makeSubPoolTest(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(10, 10, 1000);
    try {
      PrioritySchedulerInterface subPool = scheduler.makeSubPool(2);
      assertNotNull(subPool);
      assertTrue(subPool instanceof PrioritySchedulerLimiter);  // if true, test cases are covered under PrioritySchedulerLimiter unit cases
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeSubPoolFail() {
    makeSubPoolFail(new PriorityScheduledExecutorTestFactory());
  }

  public static void makeSubPoolFail(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    try {
      scheduler.makeSubPool(2);
      fail("Exception should have been thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void interruptedDuringRunTest() throws InterruptedException, TimeoutException {
    final long taskRunTime = 1000 * 10;
    final PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 1000);
    try {
      final AsyncVerifier av = new AsyncVerifier();
      TestRunnable tr = new TestRunnable() {
        @Override
        public void handleRunFinish() {
          long startTime = System.currentTimeMillis();
          Thread currentThread = Thread.currentThread();
          while (System.currentTimeMillis() - startTime < taskRunTime && 
                 ! currentThread.isInterrupted()) {
            // spin
          }
          
          av.assertTrue(currentThread.isInterrupted());
          av.signalComplete();
        }
      };
      
      ListenableFuture<?> future = executor.submit(tr);
      
      tr.blockTillStarted();
      assertEquals(1, executor.getCurrentPoolSize());
      
      // should interrupt
      assertTrue(future.cancel(true));
      av.waitForTest(); // verify thread was interruped as expected
      
      // verify worker was returned to pool
      blockTillWorkerAvailable(executor);
      
      // verify pool size is still correct
      assertEquals(1, executor.getCurrentPoolSize());
      // verify interrupted status has been cleared
      assertFalse(executor.availableWorkers.getFirst().thread.isInterrupted());
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void interruptedAfterRunTest() throws InterruptedException, TimeoutException {
    final PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 1000);
    try {
      ensureIdleWorker(executor);
      
      // send interrupt
      executor.availableWorkers.getFirst().thread.interrupt();
      
      final AsyncVerifier av = new AsyncVerifier();
      executor.execute(new TestRunnable() {
        @Override
        public void handleRunStart() {
          av.assertFalse(Thread.currentThread().isInterrupted());
          av.signalComplete();
        }
      });
      
      av.waitForTest(); // will throw an exception if invalid
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void executeTest() {
    executeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void executeTest(PriorityScheduledExecutorFactory priorityFactory) {
    try {
      SubmitterExecutorInterfaceTest.executeTest(new FactoryWrapper(priorityFactory));

      PrioritySchedulerInterface scheduler = priorityFactory.make(2, 2, 1000);
      executePriorityTest(scheduler);
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  public static void executePriorityTest(PrioritySchedulerInterface scheduler) {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    scheduler.execute(tr1, TaskPriority.High);
    scheduler.execute(tr2, TaskPriority.Low);
    scheduler.execute(tr1, TaskPriority.High);
    scheduler.execute(tr2, TaskPriority.Low);
    
    tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
    tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    submitRunnableTest(new PriorityScheduledExecutorTestFactory());
  }

  public static void submitRunnableTest(PriorityScheduledExecutorFactory priorityFactory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterfaceTest.submitRunnableTest(new FactoryWrapper(priorityFactory));

      PrioritySchedulerInterface scheduler = priorityFactory.make(2, 2, 1000);
      
      submitRunnablePriorityTest(scheduler);
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  public static void submitRunnablePriorityTest(PrioritySchedulerInterface scheduler) {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    scheduler.submit(tr1, TaskPriority.High);
    scheduler.submit(tr2, TaskPriority.Low);
    scheduler.submit(tr1, TaskPriority.High);
    scheduler.submit(tr2, TaskPriority.Low);
    
    tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
    tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    submitRunnableWithResultTest(new PriorityScheduledExecutorTestFactory());
  }

  public static void submitRunnableWithResultTest(PriorityScheduledExecutorFactory priorityFactory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(new FactoryWrapper(priorityFactory));

      PrioritySchedulerInterface scheduler = priorityFactory.make(2, 2, 1000);
      
      submitRunnableWithResultPriorityTest(scheduler);
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  public static void submitRunnableWithResultPriorityTest(PrioritySchedulerInterface scheduler) {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    scheduler.submit(tr1, tr1, TaskPriority.High);
    scheduler.submit(tr2, tr2, TaskPriority.Low);
    scheduler.submit(tr1, tr1, TaskPriority.High);
    scheduler.submit(tr2, tr2, TaskPriority.Low);
    
    tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
    tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    submitCallableTest(new PriorityScheduledExecutorTestFactory());
  }

  public static void submitCallableTest(PriorityScheduledExecutorFactory priorityFactory) throws InterruptedException, ExecutionException {
    try {
      SubmitterExecutorInterfaceTest.submitCallableTest(new FactoryWrapper(priorityFactory));

      PrioritySchedulerInterface scheduler = priorityFactory.make(2, 2, 1000);
      
      submitCallablePriorityTest(scheduler);
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  public static void submitCallablePriorityTest(PrioritySchedulerInterface scheduler) {
    TestCallable tc1 = new TestCallable(0);
    TestCallable tc2 = new TestCallable(0);
    scheduler.submit(tc1, TaskPriority.High);
    scheduler.submit(tc2, TaskPriority.Low);
    
    tc1.blockTillTrue(); // throws exception if fails
    tc2.blockTillTrue(); // throws exception if fails
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SubmitterExecutorInterfaceTest.executeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SubmitterExecutorInterfaceTest.submitRunnableFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SubmitterExecutorInterfaceTest.submitCallableFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void scheduleTest() {
    SimpleSchedulerInterfaceTest.scheduleTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void scheduleNoDelayTest() {
    SimpleSchedulerInterfaceTest.scheduleNoDelayTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(psetf);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, 
                                                             ExecutionException, 
                                                             TimeoutException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(psetf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(psetf);
  }
  
  @Test
  public void scheduleFail() {
    SimpleSchedulerInterfaceTest.scheduleFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(psetf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(psetf);
  }
  
  @Test
  public void recurringExecutionTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionTest(false, psetf);
  }
  
  @Test
  public void recurringExecutionInitialDelayTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionTest(true, psetf);
  }
  
  @Test
  public void recurringExecutionFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionFail(psetf);
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
    int runFrequency = 1;
    
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(2, 2, 1000);
    try {
      TestRunnable removedTask = new TestRunnable();
      TestRunnable keptTask = new TestRunnable();
      scheduler.scheduleWithFixedDelay(removedTask, 0, runFrequency, priority);
      scheduler.scheduleWithFixedDelay(keptTask, 0, runFrequency, priority);
      removedTask.blockTillStarted();
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(removedTask));
      
      // verify removed is no longer running, and the kept task continues to run
      int keptRunCount = keptTask.getRunCount();
      int runCount = removedTask.getRunCount();
      TestUtils.sleep(runFrequency * 10);

      // may be +1 if the task was running while the remove was called
      assertTrue(removedTask.getRunCount() == runCount || 
                 removedTask.getRunCount() == runCount + 1);
      
      assertTrue(keptTask.getRunCount() > keptRunCount);
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
      TestCallable task = new TestCallable();
      scheduler.submitScheduled(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void wrapperExecuteTest() {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterExecutorInterfaceTest.executeTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.makeSubmitterScheduler(2, false);
      
      executePriorityTest(scheduler);
    } finally {
      wf.shutdown();  // must shutdown here because we created another scheduler after calling executeTest
    }
  }
  
  @Test
  public void wrapperSubmitRunnableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterExecutorInterfaceTest.submitRunnableTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.makeSubmitterScheduler(2, false);
      
      submitRunnablePriorityTest(scheduler);
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitRunnableTest
    }
  }
  
  @Test
  public void wrapperSubmitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.makeSubmitterScheduler(2, false);
      
      submitRunnableWithResultPriorityTest(scheduler);
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitRunnableTest
    }
  }
  
  @Test
  public void wrapperSubmitCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterExecutorInterfaceTest.submitCallableTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.makeSubmitterScheduler(2, false);
      
      submitCallablePriorityTest(scheduler);
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitCallableTest
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperExecuteFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterExecutorInterfaceTest.executeFail(wf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(wf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableFail(wf);
  }
  
  @Test
  public void wrapperScheduleTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.scheduleTest(wf);
  }
  
  @Test
  public void wrapperScheduleNoDelayTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.scheduleNoDelayTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableTest() throws InterruptedException, 
                                                          ExecutionException, 
                                                          TimeoutException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableWithResultTest() throws InterruptedException, 
                                                                    ExecutionException, 
                                                                    TimeoutException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledCallableTest() throws InterruptedException, 
                                                          ExecutionException, 
                                                          TimeoutException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(wf);
  }
  
  @Test
  public void wrapperScheduleFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.scheduleFail(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(wf);
  }
  
  @Test
  public void wrapperRecurringExecutionTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(false, wf);
  }
  
  @Test
  public void wrapperRecurringExecutionInitialDelayTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(true, wf);
  }
  
  @Test
  public void wrapperRecurringExecutionFail() {
    WrapperFactory sf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }
  
  @Test
  public void shutdownTest() {
    shutdownTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void shutdownTest(PriorityScheduledExecutorFactory factory) {
    try {
      PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
      
      scheduler.shutdown();
      
      assertTrue(scheduler.isShutdown());
      
      try {
        scheduler.execute(new TestRunnable());
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
      
      try {
        scheduler.schedule(new TestRunnable(), 1000);
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
      
      try {
        scheduler.scheduleWithFixedDelay(new TestRunnable(), 100, 100);
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownNowTest() {
    shutdownNowTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void shutdownNowTest(PriorityScheduledExecutorFactory factory) {
    try {
      PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
      
      scheduler.shutdownNow();
      
      assertTrue(scheduler.isShutdown());
      
      try {
        scheduler.execute(new TestRunnable());
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
      
      try {
        scheduler.schedule(new TestRunnable(), 1000);
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
      
      try {
        scheduler.scheduleWithFixedDelay(new TestRunnable(), 100, 100);
        fail("Execption should have been thrown");
      } catch (IllegalStateException e) {
        // expected
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void addToQueueTest() {
    addToQueueTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void addToQueueTest(PriorityScheduledExecutorFactory factory) {
    long taskDelay = 1000 * 10; // make it long to prevent it from getting consumed from the queue
    
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    try {
      // verify before state
      assertFalse(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(new OneTimeTaskWrapper(new TestRunnable(), 
                                                  TaskPriority.High, 
                                                  taskDelay));

      assertEquals(1, scheduler.highPriorityQueue.size());
      assertEquals(0, scheduler.lowPriorityQueue.size());
      assertTrue(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(new OneTimeTaskWrapper(new TestRunnable(), 
                                                  TaskPriority.Low, 
                                                  taskDelay));

      assertEquals(1, scheduler.highPriorityQueue.size());
      assertEquals(1, scheduler.lowPriorityQueue.size());
      assertTrue(scheduler.highPriorityConsumer.isRunning());
      assertTrue(scheduler.lowPriorityConsumer.isRunning());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getExistingWorkerTest() {
    getExistingWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getExistingWorkerTest(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    try {
      synchronized (scheduler.workersLock) {
        // add an idle worker
        Worker testWorker = scheduler.makeNewWorker();
        scheduler.workerDone(testWorker);
        
        assertEquals(1, scheduler.availableWorkers.size());
        
        try {
          Worker returnedWorker = scheduler.getExistingWorker(100);
          assertTrue(returnedWorker == testWorker);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void lookForExpiredWorkersTest() {
    lookForExpiredWorkersTest(new PriorityScheduledExecutorTestFactory());
  }

  public static void lookForExpiredWorkersTest(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 0);
    try {
      synchronized (scheduler.workersLock) {
        // add an idle worker
        Worker testWorker = scheduler.makeNewWorker();
        scheduler.workerDone(testWorker);
        
        assertEquals(1, scheduler.availableWorkers.size());
        
        TestUtils.blockTillClockAdvances();
        Clock.accurateTime(); // update clock so scheduler will see it
        
        scheduler.expireOldWorkers();
        
        // should not have collected yet due to core size == 1
        assertEquals(1, scheduler.availableWorkers.size());
  
        scheduler.allowCoreThreadTimeOut(true);
        
        TestUtils.blockTillClockAdvances();
        Clock.accurateTime(); // update clock so scheduler will see it
        
        scheduler.expireOldWorkers();
        
        // verify collected now
        assertEquals(0, scheduler.availableWorkers.size());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public interface PriorityScheduledExecutorFactory {
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs, 
                                          TaskPriority defaultPriority, 
                                          long maxWaitForLowPrioriyt);
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs);
    public void shutdown();
  }

  private static class FactoryWrapper implements SubmitterSchedulerFactory {
    PriorityScheduledExecutorFactory factory;
    
    private FactoryWrapper(PriorityScheduledExecutorFactory factory) {
      this.factory = factory;
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
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      PriorityScheduledExecutor result = factory.make(poolSize, poolSize, 500);
      if (prestartIfAvailable) {
        result.prestartAllCoreThreads();
      }
      
      return result;
    }

    @Override
    public void shutdown() {
      factory.shutdown();
    }
  }
  
  private class PriorityScheduledExecutorTestFactory implements PriorityScheduledExecutorFactory, 
                                                                SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private PriorityScheduledExecutorTestFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }

    @Override
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize,
                                          long keepAliveTimeInMs,
                                          TaskPriority defaultPriority,
                                          long maxWaitForLowPriority) {
      PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                             keepAliveTimeInMs, defaultPriority, 
                                                                             maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs) {
      PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                             keepAliveTimeInMs);
      executors.add(result);
      
      return result;
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
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                             1000);
      if (prestartIfAvailable) {
        result.prestartAllCoreThreads();
      }
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
      }
    }
  }
  
  private class WrapperFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private WrapperFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
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
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      TaskPriority originalPriority = TaskPriority.Low;
      TaskPriority returnPriority = TaskPriority.High;
      PriorityScheduledExecutor result = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                             1000, originalPriority, 
                                                                             500);
      if (prestartIfAvailable) {
        result.prestartAllCoreThreads();
      }
      executors.add(result);
      
      return result.makeWithDefaultPriority(returnPriority);
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
