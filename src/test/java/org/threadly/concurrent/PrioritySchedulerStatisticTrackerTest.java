package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutorTest.PriorityScheduledExecutorFactory;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest {
  @Test
  public void constructorTest() {
    new PrioritySchedulerStatisticTracker(1, 1, 1000);
    new PrioritySchedulerStatisticTracker(1, 1, 1000, false);
    new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                          TaskPriority.High, 100);
    new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                          TaskPriority.High, 100, false);
    new PrioritySchedulerStatisticTracker(1, 1, 1000, TaskPriority.High, 100, 
                                          Executors.defaultThreadFactory());
  }
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
  public void getAndSetCorePoolSizeAboveMaxTest() {
    PriorityScheduledExecutorTest.getAndSetCorePoolSizeAboveMaxTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void lowerSetCorePoolSizeCleansWorkerTest() {
    PriorityScheduledExecutorTest.lowerSetCorePoolSizeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void setCorePoolSizeFail() {
    PriorityScheduledExecutorTest.setCorePoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    PriorityScheduledExecutorTest.getAndSetMaxPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetMaxPoolSizeBelowCoreTest() {
    PriorityScheduledExecutorTest.getAndSetMaxPoolSizeBelowCoreTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void lowerSetMaxPoolSizeCleansWorkerTest() {
    PriorityScheduledExecutorTest.lowerSetMaxPoolSizeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void setMaxPoolSizeFail() {
    PriorityScheduledExecutorTest.setMaxPoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void setMaxPoolSizeBlockedThreadsTest() {
    PriorityScheduledExecutorTest.setMaxPoolSizeUnblockedThreadTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    PriorityScheduledExecutorTest.getAndSetLowPriorityWaitTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    PriorityScheduledExecutorTest.setLowPriorityWaitFail(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getAndSetKeepAliveTimeTest() {
    PriorityScheduledExecutorTest.getAndSetKeepAliveTimeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void lowerSetKeepAliveTimeCleansWorkerTest() {
    PriorityScheduledExecutorTest.lowerSetKeepAliveTimeCleansWorkerTest(new PriorityScheduledExecutorTestFactory());
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
    PriorityScheduledExecutorTest.executeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTest.submitRunnableTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTest.submitRunnableWithResultTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTest.submitCallableTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterExecutorInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterExecutorInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(sf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleFail(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
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
    int runFrequency = 1;
    
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2, 2, 1000);
    try {
      TestRunnable task = new TestRunnable();
      scheduler.scheduleWithFixedDelay(task, 0, runFrequency, priority);
      task.blockTillStarted();
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
      
      // verify no longer running
      int runCount = task.getRunCount();
      TestUtils.sleep(runFrequency * 10);
      
      // may be +1 if the task was running while the remove was called
      assertTrue(task.getRunCount() == runCount || 
                 task.getRunCount() == runCount + 1);
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
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(2, 2, 1000);
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
      
      PriorityScheduledExecutorTest.executePriorityTest(scheduler);
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
      
      PriorityScheduledExecutorTest.submitRunnablePriorityTest(scheduler);
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
      
      PriorityScheduledExecutorTest.submitRunnableWithResultPriorityTest(scheduler);
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
      
      PriorityScheduledExecutorTest.submitCallablePriorityTest(scheduler);
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
  public void wrapperSubmitScheduledRunnableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableWithResultTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(wf);
  }
  
  @Test
  public void wrapperScheduleExecutionFail() {
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
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(wf);
  }
  
  @Test
  public void wrapperRecurringExecutionFail() {
    WrapperFactory sf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }
  
  @Test
  public void shutdownTest() {
    PriorityScheduledExecutorTest.shutdownTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void shutdownNowTest() {
    PriorityScheduledExecutorTest.shutdownNowTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void addToQueueTest() {
    PriorityScheduledExecutorTest.addToQueueTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void getExistingWorkerTest() {
    PriorityScheduledExecutorTest.getExistingWorkerTest(new PriorityScheduledExecutorTestFactory());
  }
  
  @Test
  public void lookForExpiredWorkersTest() {
    PriorityScheduledExecutorTest.lookForExpiredWorkersTest(new PriorityScheduledExecutorTestFactory());
  }
  
  private class SchedulerFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private SchedulerFactory() {
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
        it.next().shutdownNow();
        it.remove();
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
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
  
  // tests for statistics tracking
  
  private static void blockTillSchedulerIdle(final PrioritySchedulerStatisticTracker scheduler) {
    new TestCondition() { // block till all are finished
      @Override
      public boolean get() {
        return scheduler.getCurrentRunningCount() == 0 && 
                 ! scheduler.getRunTimes().isEmpty();
      }
    }.blockTillTrue();
  }
  
  @Test
  public void resetCollectedStatsTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      // prestart so reuse percent is not zero
      scheduler.prestartAllCoreThreads();
      TestRunnable lastRunnable = null;
      boolean flip = false;
      for (int i = 0; i < TEST_QTY; i++) {
        lastRunnable = new TestRunnable(1);
        if (flip) {
          scheduler.execute(lastRunnable, TaskPriority.High);
          flip = false;
        } else {
          scheduler.execute(lastRunnable, TaskPriority.Low);
          flip = true;
        }
      }
      
      lastRunnable.blockTillFinished();
      // block till all are finished
      blockTillSchedulerIdle(scheduler);
      
      // reset stats
      scheduler.resetCollectedStats();
      
      assertEquals(-1, scheduler.getAverageTaskRunTime(), 0);
      assertEquals(-1, scheduler.getHighPriorityAvgExecutionDelay(), 0);
      assertEquals(-1, scheduler.getHighPriorityThreadReusePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityAvgExecutionDelay(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadReusePercent(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(-1, scheduler.getAverageTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgRunTimeTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
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
      // block till all are finished
      blockTillSchedulerIdle(scheduler);
      
      List<Long> runTimes = scheduler.getRunTimes();
      assertEquals(lowPriorityCount + highPriorityCount, 
                   runTimes.size());
      
      long totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      long avgRunTime = totalRunTime / runTimes.size();
      
      assertEquals(avgRunTime, scheduler.getAverageTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
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
      
      assertEquals(0, scheduler.getCurrentRunningCount());
      
      assertEquals(lowPriorityCount + highPriorityCount, 
                   scheduler.getTotalExecutionCount());
      assertEquals(lowPriorityCount, scheduler.getLowPriorityTotalExecutionCount());
      assertEquals(highPriorityCount, scheduler.getHighPriorityTotalExecutionCount());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getThreadReusePercentTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(-1, scheduler.getThreadReusePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadReusePercent(), 0);
      assertEquals(-1, scheduler.getHighPriorityThreadReusePercent(), 0);
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertEquals(0, scheduler.getThreadReusePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadReusePercent(), 0);
      assertEquals(0, scheduler.getHighPriorityThreadReusePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertEquals(50, scheduler.getThreadReusePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadReusePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadReusePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertEquals(100, scheduler.getLowPriorityThreadReusePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadReusePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertEquals(75, scheduler.getThreadReusePercent(), 0);
      assertEquals(100, scheduler.getLowPriorityThreadReusePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadReusePercent(), 0);
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(-1, scheduler.getMedianTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
                                                                                        highPriorityCount + lowPriorityCount, 
                                                                                        1000, TaskPriority.High, 100);
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler);
      
      List<Long> samples = new ArrayList<Long>(scheduler.getRunTimes());
      Collections.sort(samples);
      
      assertEquals(0, scheduler.getMedianTaskRunTime(), samples.get(samples.size() / 2));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(-1, scheduler.getAvgExecutionDelay());
    } finally {
      scheduler.shutdownNow();
    }
  }

  public void getPriorityAvgExecutionDelayNoInputTest(TaskPriority testPriority) {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      BlockingTestRunnable br = new BlockingTestRunnable();
      switch (testPriority) { // submit with opposite priority
        case High:
          scheduler.execute(br, TaskPriority.Low);
          break;
        case Low:
          scheduler.execute(br, TaskPriority.High);
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + testPriority);
      }
      br.unblock();

      // wait for task to finish now
      br.blockTillFinished();
      blockTillSchedulerIdle(scheduler);

      switch (testPriority) {
        case High:
          assertEquals(-1, scheduler.getHighPriorityAvgExecutionDelay());
          break;
        case Low:
          assertEquals(-1, scheduler.getLowPriorityAvgExecutionDelay());
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + testPriority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAvgExecutionDelayNoInputTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayNoInputTest() {
    getPriorityAvgExecutionDelayNoInputTest(TaskPriority.Low);
  }
  

  public void getPriorityAvgExecutionDelayTest(TaskPriority priority) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler);
      
      List<Long> samples;
      switch (priority) {
        case High:
          samples = scheduler.getHighPriorityExecutionDelays();
          break;
        case Low:
          samples = scheduler.getLowPriorityExecutionDelays();
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
      
      double total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }
      
      long expectedAvg = Math.round(total / samples.size());

      switch (priority) {
        case High:
          assertEquals(expectedAvg, scheduler.getHighPriorityAvgExecutionDelay(), 0);
          break;
        case Low:
          assertEquals(expectedAvg, scheduler.getLowPriorityAvgExecutionDelay(), 0);
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityAvgExecutionDelayTest() {
    getPriorityAvgExecutionDelayTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityAvgExecutionDelayTest() {
    getPriorityAvgExecutionDelayTest(TaskPriority.Low);
  }
  

  public void getPriorityMedianExecutionDelayTest(TaskPriority priority) {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);

    assertEquals(-1, scheduler.getHighPriorityMedianExecutionDelay());
    assertEquals(-1, scheduler.getLowPriorityMedianExecutionDelay());
    
    try {
      BlockingTestRunnable lastRunnable = null;
      for (int i = 0; i < lowPriorityCount; i++) {
        if (lastRunnable != null) {
          TestUtils.blockTillClockAdvances();
          lastRunnable.unblock();
        }
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.Low);
      }
      for (int i = 0; i < highPriorityCount; i++) {
        TestUtils.blockTillClockAdvances();
        lastRunnable.unblock();
        lastRunnable = new BlockingTestRunnable();
        scheduler.execute(lastRunnable, 
                          TaskPriority.High);
      }
      TestUtils.blockTillClockAdvances();
      lastRunnable.unblock();
      
      lastRunnable.blockTillFinished();
      blockTillSchedulerIdle(scheduler);
      
      List<Long> samples;
      switch (priority) {
        case High:
          samples = new ArrayList<Long>(scheduler.getHighPriorityExecutionDelays());
          break;
        case Low:
          samples = new ArrayList<Long>(scheduler.getLowPriorityExecutionDelays());
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
      
      Collections.sort(samples);

      switch (priority) {
        case High:
          assertEquals(samples.get(samples.size() / 2), scheduler.getHighPriorityMedianExecutionDelay(), 0);
          break;
        case Low:
          assertEquals(samples.get(samples.size() / 2), scheduler.getLowPriorityMedianExecutionDelay(), 0);
          break;
        default:
          throw new UnsupportedOperationException("Priority not implenented: " + priority);
      }
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getHighPriorityMedianExecutionDelayTest() {
    getPriorityMedianExecutionDelayTest(TaskPriority.High);
  }
  
  @Test
  public void getLowPriorityMedianExecutionDelayTest() {
    getPriorityMedianExecutionDelayTest(TaskPriority.Low);
  }
  
  @Test
  public void getRunnablesRunningOverTimeTest() {
    final int checkTime = 20;
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      BlockingTestRunnable br = new BlockingTestRunnable();
      scheduler.execute(br);
      
      long before = System.currentTimeMillis();
      br.blockTillStarted();
      TestUtils.sleep(System.currentTimeMillis() - before + checkTime + 1);
      
      assertEquals(1, scheduler.getQtyRunningOverTime(checkTime));
      List<Runnable> longRunning = scheduler.getRunnablesRunningOverTime(checkTime);
      br.unblock();
      
      assertEquals(1, longRunning.size());
      assertTrue(longRunning.get(0) == br);
      
      // wait for task to finish now
      blockTillSchedulerIdle(scheduler);

      assertEquals(0, scheduler.getQtyRunningOverTime(0));
      longRunning = scheduler.getRunnablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdownNow();
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

      assertEquals(1, scheduler.getQtyRunningOverTime(checkTime));
      List<Callable<?>> longRunning = scheduler.getCallablesRunningOverTime(checkTime);
      bc.unblock();
      
      assertEquals(1, longRunning.size());
      assertTrue(longRunning.get(0) == bc);
      
      // wait for task to finish now
      blockTillSchedulerIdle(scheduler);

      assertEquals(0, scheduler.getQtyRunningOverTime(0));
      longRunning = scheduler.getCallablesRunningOverTime(0);

      assertTrue(longRunning.isEmpty());
    } finally {
      scheduler.shutdownNow();
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
        it.next().shutdownNow();
        it.remove();
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
