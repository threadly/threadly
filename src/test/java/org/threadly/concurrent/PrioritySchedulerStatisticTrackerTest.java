package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.Worker;
import org.threadly.concurrent.PriorityScheduledExecutorTest.PriorityScheduledExecutorFactory;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.TestCallable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest {
  @Test
  public void getDefaultPriorityTest() {
    PriorityScheduledExecutorTest.getDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void makeWithDefaultPriorityTest() {
    PriorityScheduledExecutorTest.makeWithDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetCorePoolSizeTest() {
    PriorityScheduledExecutorTest.getAndSetCorePoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void setCorePoolSizeFail() {
    PriorityScheduledExecutorTest.setCorePoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    PriorityScheduledExecutorTest.getAndSetMaxPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setMaxPoolSizeFail() {
    PriorityScheduledExecutorTest.setMaxPoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetKeepAliveTimeTest() {
    PriorityScheduledExecutorTest.getAndSetKeepAliveTimeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setKeepAliveTimeFail() {
    PriorityScheduledExecutorTest.setKeepAliveTimeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PriorityScheduledExecutorTest.getCurrentPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void makeSubPoolTest() {
    PriorityScheduledExecutorTest.makeSubPoolTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeSubPoolFail() {
    PriorityScheduledExecutorTest.makeSubPoolFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void executeTest() {
    PriorityScheduledExecutorTest.executeTest(new SchedulerFactory(), 
                                              new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitRunnableTest() {
    PriorityScheduledExecutorTest.submitRunnableTest(new SchedulerFactory(), 
                                                     new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTest.submitCallableTest(new SchedulerFactory(), 
                                                     new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.executeFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitRunnableFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitCallableFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledCallableTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleExecutionFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledCallableFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void wrapperExecuteTest() {
    WrapperFactory wf = new WrapperFactory();
    PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
    try {
      SimpleSchedulerInterfaceTest.executeTest(wf);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitRunnableTest() {
    WrapperFactory wf = new WrapperFactory();
    PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
    try {
      SimpleSchedulerInterfaceTest.submitRunnableTest(wf);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
    try {
      SimpleSchedulerInterfaceTest.submitCallableTest(wf);
      
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);

      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
    } finally {
      wf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperExecuteFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.executeFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitRunnableFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitCallableFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperScheduleTest() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleTest(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableTest() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledRunnableTest(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitScheduledCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledCallableTest(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperScheduleExecutionFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.scheduleExecutionFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledRunnableFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperSubmitScheduledCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.submitScheduledCallableFail(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperRecurringExecutionTest() {
    WrapperFactory wf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionTest(wf);
    } finally {
      wf.shutdown();
    }
  }
  
  @Test
  public void wrapperRecurringExecutionFail() {
    WrapperFactory sf = new WrapperFactory();
    
    try {
      SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void shutdownTest() {
    PriorityScheduledExecutor scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000);
    
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
  }
  
  @Test
  public void addToQueueTest() {
    long taskDelay = 1000 * 10; // make it long to prevent it from getting consumed from the queue
    PriorityScheduledExecutor scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000);
    try {
      // verify before state
      assertFalse(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(new OneTimeTaskWrapper(new TestRunnable(), 
                                                  TaskPriority.High, 
                                                  taskDelay));

      assertEquals(scheduler.highPriorityQueue.size(), 1);
      assertEquals(scheduler.lowPriorityQueue.size(), 0);
      assertTrue(scheduler.highPriorityConsumer.isRunning());
      assertFalse(scheduler.lowPriorityConsumer.isRunning());
      
      scheduler.addToQueue(new OneTimeTaskWrapper(new TestRunnable(), 
                                                  TaskPriority.Low, 
                                                  taskDelay));

      assertEquals(scheduler.highPriorityQueue.size(), 1);
      assertEquals(scheduler.lowPriorityQueue.size(), 1);
      assertTrue(scheduler.highPriorityConsumer.isRunning());
      assertTrue(scheduler.lowPriorityConsumer.isRunning());
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getExistingWorkerTest() {
    PriorityScheduledExecutor scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000);
    try {
      // add an idle worker
      Worker testWorker = scheduler.makeNewWorker();
      scheduler.workerDone(testWorker);
      
      assertEquals(scheduler.availableWorkers.size(), 1);
      
      try {
        Worker returnedWorker = scheduler.getExistingWorker(100);
        assertTrue(returnedWorker == testWorker);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void lookForExpiredWorkersTest() {
    PriorityScheduledExecutor scheduler = new PrioritySchedulerStatisticTracker(1, 1, 0);
    try {
      // add an idle worker
      Worker testWorker = scheduler.makeNewWorker();
      scheduler.workerDone(testWorker);
      
      assertEquals(scheduler.availableWorkers.size(), 1);
      
      TestUtils.blockTillClockAdvances();
      Clock.accurateTime(); // update clock so scheduler will see it
      
      scheduler.lookForExpiredWorkers();
      
      // should not have collected yet due to core size == 1
      assertEquals(scheduler.availableWorkers.size(), 1);

      scheduler.allowCoreThreadTimeOut(true);
      
      TestUtils.blockTillClockAdvances();
      Clock.accurateTime(); // update clock so scheduler will see it
      
      scheduler.lookForExpiredWorkers();
      
      // verify collected now
      assertEquals(scheduler.availableWorkers.size(), 0);
    } finally {
      scheduler.shutdown();
    }
  }
  
  private class SchedulerFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor result = new PrioritySchedulerStatisticTracker(poolSize, poolSize, 
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
        it.next().shutdown();
        it.remove();
      }
    }
  }
  
  private class WrapperFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private WrapperFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
      TaskPriority originalPriority = TaskPriority.Low;
      TaskPriority returnPriority = TaskPriority.High;
      PriorityScheduledExecutor result = new PrioritySchedulerStatisticTracker(poolSize, poolSize, 
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
        it.next().shutdown();
        it.remove();
      }
    }
  }
  
  // tests for statistics tracking
  
  // TODO - reduce code duplication for bellow tests, several are almost identical (just high vs low priority)
  
  @Test
  public void getAvgRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getAverageTaskRunTime(), -1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getAvgRunTimeTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
                                                                                              highPriorityCount + lowPriorityCount, 
                                                                                              1000, TaskPriority.High, 100);
    try {
      for (int i = 0; i < lowPriorityCount; i++) {
        scheduler.execute(new TestRunnable(), 
                          TaskPriority.Low);
      }
      TestRunnable lastRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastRunnable = new TestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      
      lastRunnable.blockTillFinished();
      new TestCondition() { // block till all are finished
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> runTimes = scheduler.getRunTimes();
      assertEquals(runTimes.size(), 
                   lowPriorityCount + highPriorityCount);
      
      long totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      long avgRunTime = totalRunTime / runTimes.size();
      
      assertEquals(scheduler.getAverageTaskRunTime(), avgRunTime);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
                                                                                        highPriorityCount + lowPriorityCount, 
                                                                                        1000, TaskPriority.High, 100);
    try {
      TestRunnable lastLowPriorityRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        lastLowPriorityRunnable = new TestRunnable();
        scheduler.execute(lastLowPriorityRunnable, 
                          TaskPriority.Low);
      }
      TestRunnable lastHighPriorityRunnable = null;
      for (int i = 0; i < highPriorityCount; i++) {
        lastHighPriorityRunnable = new TestRunnable();
        scheduler.execute(lastHighPriorityRunnable, 
                          TaskPriority.High);
      }
      lastLowPriorityRunnable.blockTillFinished();
      lastHighPriorityRunnable.blockTillFinished();
      
      assertEquals(scheduler.getCurrentlyRunningCount(), 0);
      
      assertEquals(scheduler.getTotalExecutionCount(), lowPriorityCount + highPriorityCount);
      assertEquals(scheduler.getLowPriorityTotalExecutionCount(), lowPriorityCount);
      assertEquals(scheduler.getHighPriorityTotalExecutionCount(), highPriorityCount);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getThreadReusePercentTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertTrue(scheduler.getThreadReusePercent() == -1);
      assertTrue(scheduler.getLowPriorityThreadReusePercent() == -1);
      assertTrue(scheduler.getHighPriorityThreadReusePercent() == -1);
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertTrue(scheduler.getThreadReusePercent() == 0);
      assertTrue(scheduler.getLowPriorityThreadReusePercent() == -1);
      assertTrue(scheduler.getHighPriorityThreadReusePercent() == 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertTrue(scheduler.getThreadReusePercent() == 50);
      assertTrue(scheduler.getLowPriorityThreadReusePercent() == -1);
      assertTrue(scheduler.getHighPriorityThreadReusePercent() == 50);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertTrue(scheduler.getLowPriorityThreadReusePercent() == 100);
      assertTrue(scheduler.getHighPriorityThreadReusePercent() == 50);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertTrue(scheduler.getThreadReusePercent() == 75);
      assertTrue(scheduler.getLowPriorityThreadReusePercent() == 100);
      assertTrue(scheduler.getHighPriorityThreadReusePercent() == 50);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getMedianTaskRunTime(), -1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
                                                                                              highPriorityCount + lowPriorityCount, 
                                                                                              1000, TaskPriority.High, 100);
    try {
      BlockRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> samples = new ArrayList<Long>(scheduler.getRunTimes());
      Collections.sort(samples);
      
      assertTrue(scheduler.getMedianTaskRunTime() == samples.get(samples.size() / 2));
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getAvgExecutionDelay(), -1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable br = new BlockRunnable();
      scheduler.execute(br, TaskPriority.Low);  // submit with opposite priority
      br.unblock();

      // wait for task to finish now
      br.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      assertEquals(scheduler.getHighPriorityAvgExecutionDelay(), -1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> samples = scheduler.getHighPriorityExecutionDelays();
      long total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }
      
      assertTrue(scheduler.getHighPriorityAvgExecutionDelay() == total / samples.size());
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable br = new BlockRunnable();
      scheduler.execute(br, TaskPriority.High);  // submit with opposite priority
      br.unblock();

      // wait for task to finish now
      br.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      assertEquals(scheduler.getLowPriorityAvgExecutionDelay(), -1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> samples = scheduler.getLowPriorityExecutionDelays();
      long total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }
      
      assertTrue(scheduler.getLowPriorityAvgExecutionDelay() == total / samples.size());
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getHighPriorityMedianExecutionDelayTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> samples = new ArrayList<Long>(scheduler.getHighPriorityExecutionDelays());
      Collections.sort(samples);
      
      assertTrue(scheduler.getHighPriorityMedianExecutionDelay() == samples.get(samples.size() / 2));
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getLowPriorityMedianExecutionDelayTest() {
    int lowPriorityCount = 5;
    int highPriorityCount = 10;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();
      
      List<Long> samples = new ArrayList<Long>(scheduler.getLowPriorityExecutionDelays());
      Collections.sort(samples);
      
      assertTrue(scheduler.getLowPriorityMedianExecutionDelay() == samples.get(samples.size() / 2));
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getRunnablesRunningOverTimeTest() {
    final int checkTime = 20;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable br = new BlockRunnable();
      scheduler.execute(br);
      
      long before = System.currentTimeMillis();
      br.blockTillStarted();
      TestUtils.sleep(System.currentTimeMillis() - before + checkTime + 1);
      
      assertEquals(scheduler.getQtyRunningOverTime(checkTime), 1);
      List<Runnable> longRunning = scheduler.getRunnablesRunningOverTime(checkTime);
      br.unblock();
      
      assertEquals(longRunning.size(), 1);
      assertTrue(longRunning.get(0) == br);
      
      // wait for task to finish now
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();

      assertEquals(scheduler.getQtyRunningOverTime(0), 0);
      longRunning = scheduler.getRunnablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getCallablesRunningOverTimeTest() {
    final int checkTime = 20;
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockCallable bc = new BlockCallable();
      scheduler.submit(bc);
      
      long before = System.currentTimeMillis();
      bc.blockTillStarted();
      TestUtils.sleep(System.currentTimeMillis() - before + checkTime + 1);

      assertEquals(scheduler.getQtyRunningOverTime(checkTime), 1);
      List<Callable<?>> longRunning = scheduler.getCallablesRunningOverTime(checkTime);
      bc.unblock();
      
      assertEquals(longRunning.size(), 1);
      assertTrue(longRunning.get(0) == bc);
      
      // wait for task to finish now
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();

      assertEquals(scheduler.getQtyRunningOverTime(0), 0);
      longRunning = scheduler.getCallablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdown();
    }
  }
  
  private class PriorityScheduledExecutorTestFactory implements PriorityScheduledExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private PriorityScheduledExecutorTestFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }

    @Override
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize,
                                          long keepAliveTimeInMs,
                                          TaskPriority defaultPriority,
                                          long maxWaitForLowPriority) {
      PriorityScheduledExecutor result = new PrioritySchedulerStatisticTracker(corePoolSize, maxPoolSize, 
                                                                               keepAliveTimeInMs, defaultPriority, 
                                                                               maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs) {
      PriorityScheduledExecutor result = new PrioritySchedulerStatisticTracker(corePoolSize, maxPoolSize, 
                                                                               keepAliveTimeInMs);
      executors.add(result);
      
      return result;
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
  
  private static class BlockRunnable extends TestRunnable {
    private volatile boolean unblock = false;
    
    public void unblock() {
      unblock = true;
    }
    
    @Override
    public void handleRunFinish() {
      while (! unblock) {
        TestUtils.sleep(10);
      }
    }
  }
  
  private static class BlockCallable extends TestCondition implements Callable<Object> {
    private volatile boolean unblock = false;
    private volatile boolean started = false;
    
    public void unblock() {
      unblock = true;
    }
    
    public void blockTillStarted() {
      this.blockTillTrue();
    }

    @Override
    public Object call() {
      started = true;
      
      while (! unblock) {
        TestUtils.sleep(10);
      }
      
      return new Object();
    }

    @Override
    public boolean get() {
      return started;
    }
  }
}
