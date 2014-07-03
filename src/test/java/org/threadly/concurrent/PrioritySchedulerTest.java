package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.TEST_QTY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduler.Worker;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.limiter.PrioritySchedulerLimiter;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerTest extends SchedulerServiceInterfaceTest {
  @Override
  protected SchedulerServiceFactory getSchedulerServiceFactory() {
    return getPrioritySchedulerFactory();
  }
  
  protected PrioritySchedulerFactory getPrioritySchedulerFactory() {
    return new PrioritySchedulerTestFactory();
  }
  
  private static void ensureIdleWorker(PriorityScheduler scheduler) {
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr);
    tr.blockTillStarted();
     
    // block till the worker is finished
    blockTillWorkerAvailable(scheduler);
    
    // verify we have a worker
    assertEquals(1, scheduler.getCurrentPoolSize());

    TestUtils.blockTillClockAdvances();
  }
  
  private static void blockTillWorkerAvailable(final PriorityScheduler scheduler) {
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    TaskPriority priority = TaskPriority.High;
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000, 
                                                                          priority, 1000);
      
      assertEquals(priority, scheduler.getDefaultPriority());
      
      priority = TaskPriority.Low;
      scheduler = factory.makePriorityScheduler(1, 1, 1000, 
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
      new PriorityScheduler(0, 1, 1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PriorityScheduler(2, 1, 1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PriorityScheduler(1, 1, -1, TaskPriority.High, 1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PriorityScheduler(1, 1, 1, TaskPriority.High, -1, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void constructorNullPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler executor = factory.makePriorityScheduler(1, 1, 1, null, 1);
      
      assertTrue(executor.getDefaultPriority() == PriorityScheduler.DEFAULT_PRIORITY);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void makeWithDefaultPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    TaskPriority originalPriority = TaskPriority.Low;
    TaskPriority newPriority = TaskPriority.High;
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000, 
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    int corePoolSize = 1;
    PriorityScheduler scheduler = factory.makePriorityScheduler(corePoolSize, 
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    int corePoolSize = 1;
    PriorityScheduler scheduler = factory.makePriorityScheduler(corePoolSize, 
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    final int poolSize = 5;
    PriorityScheduler scheduler = factory.makePriorityScheduler(poolSize, poolSize, 0); // must have no keep alive time to work
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    int corePoolSize = 1;
    int maxPoolSize = 10;
    // first construct a valid scheduler
    PriorityScheduler scheduler = factory.makePriorityScheduler(corePoolSize, 
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    final int originalCorePoolSize = 5;
    int maxPoolSize = originalCorePoolSize;
    PriorityScheduler scheduler = factory.makePriorityScheduler(originalCorePoolSize, maxPoolSize, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    final int originalPoolSize = 5;  // must be above 1
    int maxPoolSize = originalPoolSize;
    PriorityScheduler scheduler = factory.makePriorityScheduler(originalPoolSize, maxPoolSize, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    final int poolSize = 5;
    PriorityScheduler scheduler = factory.makePriorityScheduler(poolSize, poolSize, 0); // must have no keep alive time to work
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(2, 2, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      
      BlockingTestRunnable btr = new BlockingTestRunnable();
      try {
        scheduler.execute(btr);
        
        btr.blockTillStarted();
        
        TestRunnable tr = new TestRunnable();
        scheduler.execute(tr);
        // should not be able to start
        assertEquals(0, tr.getRunCount());
        
        scheduler.setMaxPoolSize(2);
        
        // tr should now be able to start, will throw exception if unable to
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long lowPriorityWait = 1000;
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, lowPriorityWait / 10, 
                                                                        TaskPriority.High, lowPriorityWait);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long lowPriorityWait = 1000;
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, lowPriorityWait / 10, 
                                                                        TaskPriority.High, lowPriorityWait);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long keepAliveTime = 1000;
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, keepAliveTime);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long keepAliveTime = 1000;
    final PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, keepAliveTime);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
    
    try {
      scheduler.setKeepAliveTime(-1L); // should throw exception for negative value
      fail("Exception should have been thrown");
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1, 1, 1000);
      // add directly to avoid starting the consumer
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                 TaskPriority.High, 0));
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.High, 0));
      
      assertEquals(2, result.getScheduledTaskCount());
      
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      
      assertEquals(4, result.getScheduledTaskCount());
      assertEquals(4, result.getScheduledTaskCount(null));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountLowPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1, 1, 1000);
      // add directly to avoid starting the consumer
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                 TaskPriority.High, 0));
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                 TaskPriority.High, 0));
      
      assertEquals(0, result.getScheduledTaskCount(TaskPriority.Low));
      
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.Low));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getScheduledTaskCountHighPriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler result = factory.makePriorityScheduler(1, 1, 1000);
      // add directly to avoid starting the consumer
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                 TaskPriority.High, 0));
      result.highPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                 TaskPriority.High, 0));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
      
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      result.lowPriorityQueue.add(result.new OneTimeTaskWrapper(new TestRunnable(), 
                                                                TaskPriority.Low, 0));
      
      assertEquals(2, result.getScheduledTaskCount(TaskPriority.High));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
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
  public void getCurrentRunningCountTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
    try {
      // verify nothing at the start
      assertEquals(0, scheduler.getCurrentRunningCount());
      
      BlockingTestRunnable btr = new BlockingTestRunnable();
      scheduler.execute(btr);
      
      btr.blockTillStarted();
      
      assertEquals(1, scheduler.getCurrentRunningCount());
      
      btr.unblock();
      
      blockTillWorkerAvailable(scheduler);
      
      assertEquals(0, scheduler.getCurrentRunningCount());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void makeSubPoolTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(10, 10, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler executor = factory.makePriorityScheduler(1, 1, 1000);
      final AsyncVerifier interruptSentAV = new AsyncVerifier();
      TestRunnable tr = new TestRunnable() {
        @Override
        public void handleRunFinish() {
          long startTime = System.currentTimeMillis();
          Thread currentThread = Thread.currentThread();
          while (System.currentTimeMillis() - startTime < taskRunTime && 
                 ! currentThread.isInterrupted()) {
            // spin
          }
          
          interruptSentAV.assertTrue(currentThread.isInterrupted());
          interruptSentAV.signalComplete();
        }
      };
      
      ListenableFuture<?> future = executor.submit(tr);
      
      tr.blockTillStarted();
      assertEquals(1, executor.getCurrentPoolSize());
      
      // should interrupt
      assertTrue(future.cancel(true));
      interruptSentAV.waitForTest(); // verify thread was interrupted as expected
      
      // verify worker was returned to pool
      blockTillWorkerAvailable(executor);
      // verify pool size is still correct
      assertEquals(1, executor.getCurrentPoolSize());
      
      // verify interrupted status has been cleared
      final AsyncVerifier interruptClearedAV = new AsyncVerifier();
      executor.execute(new Runnable() {
        @Override
        public void run() {
          interruptClearedAV.assertFalse(Thread.currentThread().isInterrupted());
          interruptClearedAV.signalComplete();
        }
      });
      // block till we have verified that the interrupted status has been reset
      interruptClearedAV.waitForTest();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void interruptedAfterRunTest() throws InterruptedException, TimeoutException {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler executor = factory.makePriorityScheduler(1, 1, 1000);
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
      factory.shutdown();
    }
  }
  
  @Override
  @Test
  public void executeTest() {
    PrioritySchedulerFactory priorityFactory = getPrioritySchedulerFactory();
    try {
      super.executeTest();

      PrioritySchedulerInterface scheduler = priorityFactory.makePriorityScheduler(2, 2, 1000);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    PrioritySchedulerFactory priorityFactory = getPrioritySchedulerFactory();
    try {
      super.submitRunnableTest();
      
      PrioritySchedulerInterface scheduler = priorityFactory.makePriorityScheduler(2, 2, 1000);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    PrioritySchedulerFactory priorityFactory = getPrioritySchedulerFactory();
    try {
      super.submitRunnableWithResultTest();

      PrioritySchedulerInterface scheduler = priorityFactory.makePriorityScheduler(2, 2, 1000);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Override
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    PrioritySchedulerFactory priorityFactory = getPrioritySchedulerFactory();
    try {
      super.submitCallableTest();

      PrioritySchedulerInterface scheduler = priorityFactory.makePriorityScheduler(2, 2, 1000);
      
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);
      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
    } finally {
      priorityFactory.shutdown();
    }
  }
  
  @Test
  public void removeHighPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.High);
  }
  
  @Test
  public void removeLowPriorityRunnableTest() {
    removeRunnableTest(TaskPriority.Low);
  }
  
  private void removeRunnableTest(TaskPriority priority) {
    int runFrequency = 1;
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
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
      
      assertTrue(keptTask.getRunCount() >= keptRunCount);
    } finally {
      factory.shutdown();
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
  
  private void removeCallableTest(TaskPriority priority) {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      TestCallable task = new TestCallable();
      scheduler.submitScheduled(task, 1000 * 10, priority);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void wrapperSamePriorityTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler highPriorityScheduler = factory.makePriorityScheduler(1, 1, 100, TaskPriority.High, 200);
      assertTrue(highPriorityScheduler.makeWithDefaultPriority(TaskPriority.High) == highPriorityScheduler);
      
      PriorityScheduler lowPriorityScheduler = factory.makePriorityScheduler(1, 1, 100, TaskPriority.Low, 200);
      assertTrue(lowPriorityScheduler.makeWithDefaultPriority(TaskPriority.Low) == lowPriorityScheduler);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void wrapperTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler highPriorityScheduler = factory.makePriorityScheduler(1, 1, 100, TaskPriority.High, 200);
      assertTrue(highPriorityScheduler.makeWithDefaultPriority(TaskPriority.Low).getDefaultPriority() == TaskPriority.Low);
      
      PriorityScheduler lowPriorityScheduler = factory.makePriorityScheduler(1, 1, 100, TaskPriority.Low, 200);
      assertTrue(lowPriorityScheduler.makeWithDefaultPriority(TaskPriority.High).getDefaultPriority() == TaskPriority.High);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void isShutdownTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      
      assertFalse(scheduler.isShutdown());
      
      scheduler.shutdown();
      
      assertTrue(scheduler.isShutdown());
      
      scheduler = factory.makePriorityScheduler(1, 1, 1000);
      scheduler.shutdownNow();
      
      assertTrue(scheduler.isShutdown());
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      TestRunnable lastRunnable = null;
      for (int i = 0; i < TEST_QTY; i++) {
        /* adding a run time to have chances that there will be 
         * runnables waiting to execute after shutdown call.
         */
        lastRunnable = new TestRunnable(5);
        scheduler.execute(lastRunnable);
      }
      
      scheduler.shutdown();
      
      // runnable should finish
      lastRunnable.blockTillFinished();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownRecurringTest() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      final PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, 0);
      
      tr.blockTillStarted();
      
      scheduler.shutdown();
      
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentPoolSize() == 0;
        }
      }.blockTillTrue();
    } finally {
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      
      scheduler.shutdown();
      
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);

      // execute one runnable which will not complete
      scheduler.execute(btr);
      
      List<TestRunnable> expectedRunnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        if (i != 0) {
          /* currently the PriorityScheduler can not remove or cancel tasks which 
           * are waiting for a worker...see issue #75
           */
          expectedRunnables.add(tr);
        }
        scheduler.execute(tr);
      }
      
      btr.blockTillStarted();
      
      List<Runnable> canceledRunnables = scheduler.shutdownNow();
      // unblock now so that others can run (if the unit test fails)
      btr.unblock();
      
      assertNotNull(canceledRunnables);
      assertTrue(canceledRunnables.containsAll(expectedRunnables));
      assertTrue(expectedRunnables.containsAll(canceledRunnables));
      
      Iterator<TestRunnable> it = expectedRunnables.iterator();
      while (it.hasNext()) {
        assertEquals(0, it.next().getRunCount());
      }
    } finally {
      btr.unblock();
      factory.shutdown();
    }
  }
  
  @Test
  public void shutdownNowFail() {
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    try {
      PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
      
      scheduler.shutdownNow();
      
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    long taskDelay = 1000 * 10; // make it long to prevent it from getting consumed from the queue
    
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
    try {
      // verify before state
      assertFalse(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(scheduler.new OneTimeTaskWrapper(new TestRunnable(), 
                                                            TaskPriority.High, 
                                                            taskDelay));

      assertEquals(1, scheduler.highPriorityQueue.size());
      assertEquals(0, scheduler.lowPriorityQueue.size());
      assertTrue(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(scheduler.new OneTimeTaskWrapper(new TestRunnable(), 
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 1000);
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
    PrioritySchedulerFactory factory = getPrioritySchedulerFactory();
    PriorityScheduler scheduler = factory.makePriorityScheduler(1, 1, 0);
    try {
      synchronized (scheduler.workersLock) {
        // add an idle worker
        Worker testWorker = scheduler.makeNewWorker();
        scheduler.workerDone(testWorker);
        
        assertEquals(1, scheduler.availableWorkers.size());
        
        TestUtils.blockTillClockAdvances();
        Clock.accurateTimeMillis(); // update clock so scheduler will see it
        
        scheduler.expireOldWorkers();
        
        // should not have collected yet due to core size == 1
        assertEquals(1, scheduler.availableWorkers.size());
  
        scheduler.allowCoreThreadTimeOut(true);
        
        TestUtils.blockTillClockAdvances();
        Clock.accurateTimeMillis(); // update clock so scheduler will see it
        
        scheduler.expireOldWorkers();
        
        // verify collected now
        assertEquals(0, scheduler.availableWorkers.size());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public interface PrioritySchedulerFactory extends SchedulerServiceFactory {
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize, 
                                                   long keepAliveTimeInMs, 
                                                   TaskPriority defaultPriority, 
                                                   long maxWaitForLowPrioriyt);
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize, 
                                                   long keepAliveTimeInMs);
  }
  
  private static class PrioritySchedulerTestFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduler> executors;
    
    private PrioritySchedulerTestFactory() {
      executors = new LinkedList<PriorityScheduler>();
    }

    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize,
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduler result = makePriorityScheduler(poolSize, poolSize, Long.MAX_VALUE);
      if (prestartIfAvailable) {
        result.prestartAllCoreThreads();
      }
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize,
                                                   long keepAliveTimeInMs,
                                                   TaskPriority defaultPriority,
                                                   long maxWaitForLowPriority) {
      PriorityScheduler result = new StrictPriorityScheduler(corePoolSize, maxPoolSize, 
                                                             keepAliveTimeInMs, defaultPriority, 
                                                             maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduler makePriorityScheduler(int corePoolSize, int maxPoolSize, 
                                                   long keepAliveTimeInMs) {
      PriorityScheduler result = new StrictPriorityScheduler(corePoolSize, maxPoolSize, 
                                                             keepAliveTimeInMs);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduler> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
      }
    }
  }
}
