package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.threadly.concurrent.PriorityScheduledExecutor.OneTimeTaskWrapper;
import org.threadly.concurrent.PriorityScheduledExecutor.Worker;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.PrioritySchedulerFactory;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest.TestCallable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorTest {
  @Test
  public void getDefaultPriorityTest() {
    getDefaultPriorityTest(new PriorityScheduledExecutorTestFactory());
  } 
  
  public static void getDefaultPriorityTest(PriorityScheduledExecutorFactory factory) {
    TaskPriority priority = TaskPriority.High;
    try {
      PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000, 
                                                         priority, 1000);
      
      assertEquals(scheduler.getDefaultPriority(), priority);
      
      priority = TaskPriority.Low;
      scheduler = factory.make(1, 1, 1000, 
                               priority, 1000);
      assertEquals(scheduler.getDefaultPriority(), priority);
    } finally {
      factory.shutdown();
    }
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
      assertEquals(newScheduler.getDefaultPriority(), newPriority);
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
      assertEquals(scheduler.getCorePoolSize(), corePoolSize);
      
      corePoolSize = 10;
      scheduler.setMaxPoolSize(corePoolSize + 10);
      scheduler.setCorePoolSize(corePoolSize);
      
      assertEquals(scheduler.getCorePoolSize(), corePoolSize);
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
      // verify can't be set higher than max size
      try {
        scheduler.setCorePoolSize(maxPoolSize + 1);
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
    int maxPoolSize = 1;
    PriorityScheduledExecutor scheduler = factory.make(1, maxPoolSize, 1000);
    try {
      assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
      
      maxPoolSize = 10;
      scheduler.setMaxPoolSize(maxPoolSize);
      
      assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
    } finally {
      factory.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setMaxPoolSizeFail() {
    setMaxPoolSizeFail(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void setMaxPoolSizeFail(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    
    try {
      scheduler.setMaxPoolSize(-1); // should throw exception for negative value
      fail("Exception should have been thrown");
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
      assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
      
      keepAliveTime = Long.MAX_VALUE;
      scheduler.setKeepAliveTime(keepAliveTime);
      
      assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
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
  public void getCurrentPoolSizeTest() {
    getCurrentPoolSizeTest(new PriorityScheduledExecutorTestFactory());
  }
  
  public static void getCurrentPoolSizeTest(PriorityScheduledExecutorFactory factory) {
    PriorityScheduledExecutor scheduler = factory.make(1, 1, 1000);
    try {
      // verify nothing at the start
      assertEquals(scheduler.getCurrentPoolSize(), 0);
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      
      tr.blockTillFinished();  // wait for execution
      
      assertEquals(scheduler.getCurrentPoolSize(), 1);
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
  public void executeTest() {
    executeTest(new SchedulerFactory(), 
                new PriorityScheduledExecutorTestFactory());
  }

  public static void executeTest(PrioritySchedulerFactory schedulerFactory, 
                                 PriorityScheduledExecutorFactory priorityFactory) {
    try {
      SimpleSchedulerInterfaceTest.executeTest(schedulerFactory);

      PriorityScheduledExecutor scheduler = priorityFactory.make(2, 2, 1000);
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
  
  @Test
  public void submitRunnableTest() {
    submitRunnableTest(new SchedulerFactory(), 
                       new PriorityScheduledExecutorTestFactory());
  }

  public static void submitRunnableTest(PrioritySchedulerFactory schedulerFactory, 
                                        PriorityScheduledExecutorFactory priorityFactory) {
    try {
      SimpleSchedulerInterfaceTest.submitRunnableTest(schedulerFactory);

      PriorityScheduledExecutor scheduler = priorityFactory.make(2, 2, 1000);
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
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    submitCallableTest(new SchedulerFactory(), 
                       new PriorityScheduledExecutorTestFactory());
  }

  public static void submitCallableTest(PrioritySchedulerFactory schedulerFactory, 
                                        PriorityScheduledExecutorFactory priorityFactory) throws InterruptedException, ExecutionException {
    try {
      SimpleSchedulerInterfaceTest.submitCallableTest(schedulerFactory);

      PriorityScheduledExecutor scheduler = priorityFactory.make(2, 2, 1000);
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
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.executeFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.scheduleExecutionFail(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerFactory sf = new SchedulerFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableFail(sf);
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
  public void wrapperSubmitRunnableTest() {
    WrapperFactory wf = new WrapperFactory();
    try {
      SimpleSchedulerInterfaceTest.submitRunnableTest(wf);

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
  public void wrapperSubmitCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    try {
      SimpleSchedulerInterfaceTest.submitCallableTest(wf);

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
    
    SimpleSchedulerInterfaceTest.submitRunnableFail(wf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void wrapperSubmitCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.submitCallableFail(wf);
  }
  
  @Test
  public void wrapperScheduleTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.scheduleTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableTest() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableTest(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledCallableTest() throws InterruptedException, ExecutionException {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableTest(wf);
  }
  
  @Test
  public void wrapperScheduleExecutionFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.scheduleExecutionFail(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledRunnableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledRunnableFail(wf);
  }
  
  @Test
  public void wrapperSubmitScheduledCallableFail() {
    WrapperFactory wf = new WrapperFactory();
    
    SimpleSchedulerInterfaceTest.submitScheduledCallableFail(wf);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
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
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 0);
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
  
  public interface PriorityScheduledExecutorFactory {
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs, 
                                          TaskPriority defaultPriority, 
                                          long maxWaitForLowPrioriyt);
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs);
    public void shutdown();
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
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                       keepAliveTimeInMs, defaultPriority, 
                                                                       maxWaitForLowPriority);
      executors.add(result);
      
      return result;
    }

    @Override
    public PriorityScheduledExecutor make(int corePoolSize, int maxPoolSize, 
                                          long keepAliveTimeInMs) {
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(corePoolSize, maxPoolSize, 
                                                                       keepAliveTimeInMs);
      executors.add(result);
      
      return result;
    }

    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
      }
    }
  }
  
  private class SchedulerFactory implements PrioritySchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    
    private SchedulerFactory() {
      executors = new LinkedList<PriorityScheduledExecutor>();
    }
    
    @Override
    public SimpleSchedulerInterface make(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(poolSize, poolSize, 
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
      PriorityScheduledExecutor result = new PriorityScheduledExecutor(poolSize, poolSize, 
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
}
