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
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.PriorityScheduledExecutorTest.PriorityScheduledExecutorFactory;
import org.threadly.concurrent.SchedulerServiceInterfaceTest.SchedulerServiceFactory;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class PrioritySchedulerStatisticTrackerTest {
  @BeforeClass
  public static void classSetup() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  @SuppressWarnings("unused")
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
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getDefaultPriorityTest(psetf);
  }
  
  @Test
  public void makeWithDefaultPriorityTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.makeWithDefaultPriorityTest(psetf);
  }
  
  @Test
  public void getAndSetCorePoolSizeTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetCorePoolSizeTest(psetf);
  }
  
  @Test
  public void getAndSetCorePoolSizeAboveMaxTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetCorePoolSizeAboveMaxTest(psetf);
  }
  
  @Test
  public void lowerSetCorePoolSizeCleansWorkerTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.lowerSetCorePoolSizeCleansWorkerTest(psetf);
  }
  
  @Test
  public void setCorePoolSizeFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.setCorePoolSizeFail(psetf);
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetMaxPoolSizeTest(psetf);
  }
  
  @Test
  public void getAndSetMaxPoolSizeBelowCoreTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetMaxPoolSizeBelowCoreTest(psetf);
  }
  
  @Test
  public void lowerSetMaxPoolSizeCleansWorkerTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.lowerSetMaxPoolSizeCleansWorkerTest(psetf);
  }
  
  @Test
  public void setMaxPoolSizeFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.setMaxPoolSizeFail(psetf);
  }
  
  @Test
  public void setMaxPoolSizeBlockedThreadsTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.setMaxPoolSizeUnblockedThreadTest(psetf);
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetLowPriorityWaitTest(psetf);
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.setLowPriorityWaitFail(psetf);
  }
  
  @Test
  public void getAndSetKeepAliveTimeTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getAndSetKeepAliveTimeTest(psetf);
  }
  
  @Test
  public void lowerSetKeepAliveTimeCleansWorkerTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.lowerSetKeepAliveTimeCleansWorkerTest(psetf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setKeepAliveTimeFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.setKeepAliveTimeFail(psetf);
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getCurrentPoolSizeTest(psetf);
  }
  
  @Test
  public void getCurrentRunningCountTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getCurrentRunningCountTest(psetf);
  }
  
  @Test
  public void makeSubPoolTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.makeSubPoolTest(psetf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeSubPoolFail() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.makeSubPoolFail(psetf);
  }
  
  @Test
  public void executeTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.executeTest(psetf);
  }
  
  @Test
  public void executeWithFailureRunnableTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.executeWithFailureRunnableTest(psetf);
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.submitRunnableTest(psetf);
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.submitRunnableWithResultTest(psetf);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.submitCallableTest(psetf);
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
  public void submitRunnableWithResultFail() {
    SchedulerFactory sf = new SchedulerFactory();
    SubmitterExecutorInterfaceTest.submitRunnableWithResultFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    SubmitterExecutorInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleTest() {
    SchedulerFactory sf = new SchedulerFactory();
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleNoDelayTest() {
    SchedulerFactory sf = new SchedulerFactory();
    SimpleSchedulerInterfaceTest.scheduleNoDelayTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    SchedulerFactory sf = new SchedulerFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() throws InterruptedException, 
                                                             ExecutionException, 
                                                             TimeoutException {
    SchedulerFactory sf = new SchedulerFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableWithResultTest(sf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, 
                                                   ExecutionException, 
                                                   TimeoutException {
    SchedulerFactory sf = new SchedulerFactory();
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void scheduleFail() {
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
    SimpleSchedulerInterfaceTest.recurringExecutionTest(false, sf);
  }
  
  @Test
  public void recurringExecutionInitialDelayTest() {
    SchedulerFactory sf = new SchedulerFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionTest(true, sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }
  
  @Test
  public void removeRunnableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    SchedulerServiceInterfaceTest.removeRunnableTest(sf);
  }
  
  @Test
  public void removeCallableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    SchedulerServiceInterfaceTest.removeCallableTest(sf);
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
  public void wrapperExecuteWithFailureRunnableTest() {
    SubmitterExecutorInterfaceTest.executeWithFailureRunnableTest(new WrapperFactory());
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
  public void wrapperSubmitRunnableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableExceptionTest(new WrapperFactory());
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
  public void wrapperSubmitRunnableWithResultExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitRunnableWithResultExceptionTest(new WrapperFactory());
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
  
  @Test
  public void wrapperSubmitCallableExceptionTest() throws InterruptedException {
    SubmitterExecutorInterfaceTest.submitCallableExceptionTest(new WrapperFactory());
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
  public void wrapperSubmitRunnableWithResultFail() {
    WrapperFactory wf = new WrapperFactory();
    SubmitterExecutorInterfaceTest.submitRunnableWithResultFail(wf);
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
    WrapperFactory wf = new WrapperFactory();
    SimpleSchedulerInterfaceTest.recurringExecutionFail(wf);
  }
  
  @Test
  public void wrapperRemoveRunnableTest() {
    WrapperFactory wf = new WrapperFactory();
    SchedulerServiceInterfaceTest.removeRunnableTest(wf);
  }
  
  @Test
  public void wrapperRemoveCallableTest() {
    WrapperFactory wf = new WrapperFactory();
    SchedulerServiceInterfaceTest.removeCallableTest(wf);
  }
  
  @Test
  public void shutdownTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.shutdownTest(psetf);
  }
  
  @Test
  public void shutdownNowTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.shutdownNowTest(psetf);
  }
  
  @Test
  public void addToQueueTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.addToQueueTest(psetf);
  }
  
  @Test
  public void getExistingWorkerTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.getExistingWorkerTest(psetf);
  }
  
  @Test
  public void lookForExpiredWorkersTest() {
    PriorityScheduledExecutorTestFactory psetf = new PriorityScheduledExecutorTestFactory();
    PriorityScheduledExecutorTest.lookForExpiredWorkersTest(psetf);
  }
  
  private class SchedulerFactory implements SchedulerServiceFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, 
                                                        boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
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
  
  private class WrapperFactory implements SchedulerServiceFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private WrapperFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }

    @Override
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, 
                                                        boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      return makeSchedulerService(poolSize, prestartIfAvailable);
    }

    @Override
    public SchedulerServiceInterface makeSchedulerService(int poolSize, boolean prestartIfAvailable) {
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
  
  private static void blockTillSchedulerIdle(final PrioritySchedulerStatisticTracker scheduler, 
                                             final int expectedSampleSize) {
    new TestCondition() { // block till all are finished
      @Override
      public boolean get() {
        return scheduler.getCurrentRunningCount() == 0 && 
                 scheduler.getRunTimes().size() >= expectedSampleSize;
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
      blockTillSchedulerIdle(scheduler, TEST_QTY);
      
      // reset stats
      scheduler.resetCollectedStats();
      
      assertEquals(-1, scheduler.getAverageTaskRunTime(), 0);
      assertEquals(-1, scheduler.getHighPriorityAvgExecutionDelay(), 0);
      assertEquals(-1, scheduler.getHighPriorityThreadAvailablePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityAvgExecutionDelay(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadAvailablePercent(), 0);
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
      int expectedCount = lowPriorityCount + highPriorityCount;
      // block till all are finished
      blockTillSchedulerIdle(scheduler, expectedCount);
      
      List<Long> runTimes = scheduler.getRunTimes();
      assertEquals(expectedCount, 
                   runTimes.size());
      
      double totalRunTime = 0;
      Iterator<Long> it = runTimes.iterator();
      while (it.hasNext()) {
        totalRunTime += it.next();
      }
      
      long avgRunTime = Math.round(totalRunTime / runTimes.size());
      
      assertEquals(avgRunTime, scheduler.getAverageTaskRunTime());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getTotalExecutionCountTest() {
    int lowPriorityCount = TEST_QTY;
    int highPriorityCount = TEST_QTY * 2;
    final PrioritySchedulerStatisticTracker scheduler;
    scheduler = new PrioritySchedulerStatisticTracker(highPriorityCount + lowPriorityCount, 
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
      
      // should not be very long after waiting above
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentRunningCount() == 0;
        }
      }.blockTillTrue();
      
      assertEquals(lowPriorityCount + highPriorityCount, 
                   scheduler.getTotalExecutionCount());
      assertEquals(lowPriorityCount, scheduler.getLowPriorityTotalExecutionCount());
      assertEquals(highPriorityCount, scheduler.getHighPriorityTotalExecutionCount());
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getThreadAvailablePercentTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(-1, scheduler.getThreadAvailablePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadAvailablePercent(), 0);
      assertEquals(-1, scheduler.getHighPriorityThreadAvailablePercent(), 0);
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertEquals(0, scheduler.getThreadAvailablePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadAvailablePercent(), 0);
      assertEquals(0, scheduler.getHighPriorityThreadAvailablePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.High);
      tr.blockTillFinished();
      
      assertEquals(50, scheduler.getThreadAvailablePercent(), 0);
      assertEquals(-1, scheduler.getLowPriorityThreadAvailablePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadAvailablePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertEquals(100, scheduler.getLowPriorityThreadAvailablePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadAvailablePercent(), 0);
      
      tr = new TestRunnable();
      scheduler.execute(tr, TaskPriority.Low);
      tr.blockTillFinished();
      
      assertEquals(75, scheduler.getThreadAvailablePercent(), 0);
      assertEquals(100, scheduler.getLowPriorityThreadAvailablePercent(), 0);
      assertEquals(50, scheduler.getHighPriorityThreadAvailablePercent(), 0);
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
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
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
      blockTillSchedulerIdle(scheduler, 1);

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
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
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
          assertEquals(expectedAvg, scheduler.getHighPriorityAvgExecutionDelay());
          break;
        case Low:
          assertEquals(expectedAvg, scheduler.getLowPriorityAvgExecutionDelay());
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
      blockTillSchedulerIdle(scheduler, lowPriorityCount + highPriorityCount);
      
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
      blockTillSchedulerIdle(scheduler, 1);

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
      blockTillSchedulerIdle(scheduler, 1);

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
