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
    TaskPriority priority = TaskPriority.High;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000, 
                                                                        priority, 1000);
    try {
      assertEquals(scheduler.getDefaultPriority(), priority);
      scheduler.shutdown();
      
      priority = TaskPriority.Low;
      scheduler = new PriorityScheduledExecutor(1, 1, 1000, 
                                                priority, 1000);
      assertEquals(scheduler.getDefaultPriority(), priority);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void makeWithDefaultPriorityTest() {
    TaskPriority originalPriority = TaskPriority.Low;
    TaskPriority newPriority = TaskPriority.High;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000, 
                                                                        originalPriority, 1000);
    assertTrue(scheduler.makeWithDefaultPriority(originalPriority) == scheduler);
    PrioritySchedulerInterface newScheduler = scheduler.makeWithDefaultPriority(newPriority);
    try {
      assertEquals(newScheduler.getDefaultPriority(), newPriority);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getAndSetCorePoolSizeTest() {
    int corePoolSize = 1;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(corePoolSize, 
                                                                        corePoolSize + 10, 1000);
    try {
      assertEquals(scheduler.getCorePoolSize(), corePoolSize);
      
      corePoolSize = 10;
      scheduler.setMaxPoolSize(corePoolSize + 10);
      scheduler.setCorePoolSize(corePoolSize);
      
      assertEquals(scheduler.getCorePoolSize(), corePoolSize);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void setCorePoolSizeFail() {
    int corePoolSize = 1;
    int maxPoolSize = 10;
    // first construct a valid scheduler
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(corePoolSize, 
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
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    int maxPoolSize = 1;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, maxPoolSize, 1000);
    try {
      assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
      
      maxPoolSize = 10;
      scheduler.setMaxPoolSize(maxPoolSize);
      
      assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setMaxPoolSizeFail() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    
    try {
      scheduler.setMaxPoolSize(-1); // should throw exception for negative value
      fail("Exception should have been thrown");
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getAndSetKeepAliveTimeTest() {
    long keepAliveTime = 1000;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, keepAliveTime);
    try {
      assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
      
      keepAliveTime = Long.MAX_VALUE;
      scheduler.setKeepAliveTime(keepAliveTime);
      
      assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setKeepAliveTimeFail() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    
    try {
      scheduler.setKeepAliveTime(-1L); // should throw exception for negative value
      fail("Exception should have been thrown");
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void getCurrentPoolSizeTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    try {
      // verify nothing at the start
      assertEquals(scheduler.getCurrentPoolSize(), 0);
      
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      
      tr.blockTillFinished();  // wait for execution
      
      assertEquals(scheduler.getCurrentPoolSize(), 1);
    } finally {
      scheduler.shutdown();
    }
  }
  
  @Test
  public void makeSubPoolTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(10, 10, 1000);
    PrioritySchedulerInterface subPool = scheduler.makeSubPool(2);
    assertNotNull(subPool);
    assertTrue(subPool instanceof PrioritySchedulerLimiter);  // if true, test cases are covered under PrioritySchedulerLimiter unit cases
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeSubPoolFail() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    scheduler.makeSubPool(2);
    fail("Exception should have been thrown");
  }
  
  @Test
  public void executeTest() {
    SchedulerFactory sf = new SchedulerFactory();
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);;
    try {
      SimpleSchedulerInterfaceTest.executeTest(sf);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      scheduler.execute(tr1, TaskPriority.High);
      scheduler.execute(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      try {
        sf.shutdown();
      } finally {
        scheduler.shutdown();
      }
    }
  }
  
  @Test
  public void submitRunnableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);;
    try {
      SimpleSchedulerInterfaceTest.submitRunnableTest(sf);
      
      TestRunnable tr1 = new TestRunnable();
      TestRunnable tr2 = new TestRunnable();
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      scheduler.submit(tr1, TaskPriority.High);
      scheduler.submit(tr2, TaskPriority.Low);
      
      tr1.blockTillFinished(1000 * 10, 2); // throws exception if fails
      tr2.blockTillFinished(1000 * 10, 2); // throws exception if fails
    } finally {
      try {
        sf.shutdown();
      } finally {
        scheduler.shutdown();
      }
    }
  }
  
  @Test
  public void submitCallableTest() {
    SchedulerFactory sf = new SchedulerFactory();
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);;
    try {
      SimpleSchedulerInterfaceTest.submitRunnableTest(sf);
      
      TestCallable tc1 = new TestCallable(0);
      TestCallable tc2 = new TestCallable(0);
      scheduler.submit(tc1, TaskPriority.High);
      scheduler.submit(tc2, TaskPriority.Low);
      
      tc1.blockTillTrue(); // throws exception if fails
      tc2.blockTillTrue(); // throws exception if fails
    } finally {
      try {
        sf.shutdown();
      } finally {
        scheduler.shutdown();
      }
    }
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
    
    private void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
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
    
    private void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdown();
      }
    }
  }
}
