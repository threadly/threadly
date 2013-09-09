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
import org.threadly.concurrent.PriorityScheduledExecutorTest.PriorityScheduledExecutorFactory;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

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
  
  @Test
  public void setMaxPoolSizeFail() {
    PriorityScheduledExecutorTest.setMaxPoolSizeFail(new PriorityScheduledExecutorTestFactory());
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
    
    SimpleSchedulerInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SubmitterSchedulerInterfaceTest.submitCallableFail(sf);
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
  public void wrapperExecuteTest() {
    WrapperFactory wf = new WrapperFactory();
    try {
      SimpleSchedulerInterfaceTest.executeTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      wf.shutdown();  // must shutdown here because we created another scheduler after calling executeTest
    }
  }
  
  @Test
  public void wrapperSubmitRunnableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterSchedulerInterfaceTest.submitRunnableTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitRunnableTest
    }
  }
  
  @Test
  public void wrapperSubmitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterSchedulerInterfaceTest.submitRunnableWithResultTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      scheduler.submit(tr1, tr1, TaskPriority.High);
      scheduler.submit(tr2, tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitRunnableTest
    }
  }
  
  @Test
  public void wrapperSubmitCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SubmitterSchedulerInterfaceTest.submitCallableTest(wf);

      PrioritySchedulerInterface scheduler = (PrioritySchedulerInterface)wf.make(2, false);
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);

      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
    } finally {
      wf.shutdown();  // must call shutdown here because we called make after submitCallableTest
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperExecuteFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.executeFail(wf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitRunnableFail(wf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SubmitterSchedulerInterfaceTest.submitCallableFail(wf);
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
    public SubmitterSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
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
    public SubmitterSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
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
  
  @Test
  public void getAvgRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getAverageTaskRunTime(), -1);
    } finally {
      scheduler.shutdownNow();
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
      scheduler.shutdownNow();
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
      scheduler.shutdownNow();
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
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getMedianTaskRunTimeNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getMedianTaskRunTime(), -1);
    } finally {
      scheduler.shutdownNow();
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
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void getAvgExecutionDelayNoInputTest() {
    PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                        TaskPriority.High, 100);
    try {
      assertEquals(scheduler.getAvgExecutionDelay(), -1);
    } finally {
      scheduler.shutdownNow();
    }
  }

  public void getPriorityAvgExecutionDelayNoInputTest(TaskPriority testPriority) {
    final PrioritySchedulerStatisticTracker scheduler = new PrioritySchedulerStatisticTracker(1, 1, 1000, 
                                                                                              TaskPriority.High, 100);
    try {
      BlockRunnable br = new BlockRunnable();
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
      new TestCondition() {
        @Override
        public boolean get() {
          return scheduler.getCurrentlyRunningCount() == 0;
        }
      }.blockTillTrue();

      switch (testPriority) {
        case High:
          assertEquals(scheduler.getHighPriorityAvgExecutionDelay(), -1);
          break;
        case Low:
          assertEquals(scheduler.getLowPriorityAvgExecutionDelay(), -1);
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
      
      long total = 0;
      Iterator<Long> it = samples.iterator();
      while (it.hasNext()) {
        total += it.next();
      }

      switch (priority) {
        case High:
          assertTrue(scheduler.getHighPriorityAvgExecutionDelay() == total / samples.size());
          break;
        case Low:
          assertTrue(scheduler.getLowPriorityAvgExecutionDelay() == total / samples.size());
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
          assertTrue(scheduler.getHighPriorityMedianExecutionDelay() == samples.get(samples.size() / 2));
          break;
        case Low:
          assertTrue(scheduler.getLowPriorityMedianExecutionDelay() == samples.get(samples.size() / 2));
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
