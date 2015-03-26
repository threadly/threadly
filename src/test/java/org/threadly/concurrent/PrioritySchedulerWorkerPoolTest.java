package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler.Worker;
import org.threadly.concurrent.PriorityScheduler.WorkerPool;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class PrioritySchedulerWorkerPoolTest {
  protected static int DEFAULT_KEEP_ALIVE_TIME = 1000;
  
  protected WorkerPool workerPool;
  
  @Before
  public void setup() {
    workerPool = new WorkerPool(new ConfigurableThreadFactory(), 1, 1, DEFAULT_KEEP_ALIVE_TIME, 
                                PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
  }
  
  @After
  public void cleanup() {
    workerPool.startShutdown();
    workerPool.finishShutdown();
    workerPool = null;
  }
  
  @Test
  public void shutdownStartTest() {
    assertFalse(workerPool.isShutdownStarted());
    assertTrue(workerPool.startShutdown());
    assertTrue(workerPool.isShutdownStarted());
  }
  
  @Test
  public void shutdownFinishTest() {
    assertFalse(workerPool.isShutdownFinished());
    workerPool.finishShutdown();
    assertTrue(workerPool.isShutdownFinished());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void getAndSetCorePoolSizeTest() {
    int corePoolSize = 10;
    workerPool.setMaxPoolSize(corePoolSize + 10);
    workerPool.setPoolSize(corePoolSize);
      
    assertEquals(corePoolSize, workerPool.corePoolSize);
  }
  
  @Test
  public void getAndSetPoolSizeTest() {
    int corePoolSize = 10;
    workerPool.setPoolSize(corePoolSize);
      
    assertEquals(corePoolSize, workerPool.getMaxPoolSize());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void getAndSetCorePoolSizeAboveMaxTest() {
    int corePoolSize = workerPool.getMaxPoolSize() * 2;
    workerPool.setPoolSize(corePoolSize);
    
    assertEquals(corePoolSize, workerPool.corePoolSize);
    assertEquals(corePoolSize, workerPool.getMaxPoolSize());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void lowerSetCorePoolSizeCleansWorkerTest() {
    workerPool.setKeepAliveTime(0);
    
    workerPool.setPoolSize(5);
    workerPool.prestartAllThreads();
    // must allow core thread timeout for this to work
    workerPool.allowCoreThreadTimeOut(true);
    TestUtils.blockTillClockAdvances();
    
    workerPool.setPoolSize(1);
    
    // verify worker was cleaned up
    assertEquals(0, workerPool.getCurrentPoolSize());
  }
  
  @Test
  public void setCorePoolSizeFail() {
    // verify no negative values
    try {
      workerPool.setPoolSize(-1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException expected) {
      // ignored
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void getAndSetMaxPoolSizeTest() {
    int newMaxPoolSize = workerPool.getMaxPoolSize() * 2;
    workerPool.setMaxPoolSize(newMaxPoolSize);
    
    assertEquals(newMaxPoolSize, workerPool.getMaxPoolSize());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void getAndSetMaxPoolSizeBelowCoreTest() {
    workerPool.setPoolSize(5);
    workerPool.setMaxPoolSize(1);
    
    assertEquals(1, workerPool.getMaxPoolSize());
    assertEquals(1, workerPool.corePoolSize);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void lowerSetMaxPoolSizeCleansWorkerTest() {
    workerPool.setKeepAliveTime(0);
    
    workerPool.setPoolSize(5);
    workerPool.prestartAllThreads();
    // must allow core thread timeout for this to work
    workerPool.allowCoreThreadTimeOut(true);
    TestUtils.blockTillClockAdvances();
    
    workerPool.setMaxPoolSize(1);
    
    // verify worker was cleaned up
    assertEquals(0, workerPool.getCurrentPoolSize());
  }
  
  @SuppressWarnings("deprecation")
  @Test (expected = IllegalArgumentException.class)
  public void setMaxPoolSizeFail() {
    workerPool.setMaxPoolSize(-1); // should throw exception for negative value
    fail("Exception should have been thrown");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setPoolSizeFail() {
    workerPool.setPoolSize(-1); // should throw exception for negative value
    fail("Exception should have been thrown");
  }
  
  @Test
  public void getAndSetLowPriorityWaitTest() {
    assertEquals(PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, workerPool.getMaxWaitForLowPriority());
    
    long lowPriorityWait = Long.MAX_VALUE;
    workerPool.setMaxWaitForLowPriority(lowPriorityWait);
    
    assertEquals(lowPriorityWait, workerPool.getMaxWaitForLowPriority());
  }
  
  @Test
  public void setLowPriorityWaitFail() {
    try {
      workerPool.setMaxWaitForLowPriority(-1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    
    assertEquals(PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS, workerPool.getMaxWaitForLowPriority());
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void getAndSetKeepAliveTimeTest() {
    assertEquals(DEFAULT_KEEP_ALIVE_TIME, workerPool.keepAliveTimeInMs);
    
    long keepAliveTime = Long.MAX_VALUE;
    workerPool.setKeepAliveTime(keepAliveTime);
    
    assertEquals(keepAliveTime, workerPool.keepAliveTimeInMs);
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void lowerSetKeepAliveTimeCleansWorkerTest() {
    workerPool.prestartAllThreads();
    // must allow core thread timeout for this to work
    workerPool.allowCoreThreadTimeOut(true);
    TestUtils.blockTillClockAdvances();
    
    workerPool.setKeepAliveTime(0);
    
    // verify worker was cleaned up
    assertEquals(0, workerPool.getCurrentPoolSize());
  }
  
  @SuppressWarnings("deprecation")
  @Test (expected = IllegalArgumentException.class)
  public void setKeepAliveTimeFail() {
    workerPool.setKeepAliveTime(-1L); // should throw exception for negative value
    fail("Exception should have been thrown");
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void allowCoreThreadTimeOutTest() {
    int corePoolSize = 5;
    
    workerPool.setPoolSize(corePoolSize);
    workerPool.prestartAllThreads();
    workerPool.setKeepAliveTime(0);
    
    workerPool.allowCoreThreadTimeOut(false);
    TestUtils.blockTillClockAdvances();
    synchronized (workerPool.workersLock) {
      workerPool.expireOldWorkers();
    }
    
    assertEquals(corePoolSize, workerPool.getCurrentPoolSize());
    
    workerPool.allowCoreThreadTimeOut(true);
    TestUtils.blockTillClockAdvances();
    synchronized (workerPool.workersLock) {
      workerPool.expireOldWorkers();
    }
    
    assertEquals(0, workerPool.getCurrentPoolSize());
  }
  
  @Test
  public void prestartAllThreadsTest() {
    int corePoolSize = 5;
    workerPool.setPoolSize(corePoolSize);
    
    assertEquals(0, workerPool.getCurrentPoolSize());
    
    workerPool.prestartAllThreads();
    
    assertEquals(corePoolSize, workerPool.getCurrentPoolSize());
  }
  
  @Test
  public void makeNewWorkerTest() {
    assertEquals(0, workerPool.getCurrentPoolSize());
    
    Worker w = workerPool.makeNewWorker();
    assertNotNull(w);
    assertTrue(w.thread.isAlive());
    assertEquals(1, workerPool.getCurrentPoolSize());
  }
  
  @Test
  public void getExistingWorkerTest() {
    synchronized (workerPool.workersLock) {
      // add an idle worker
      Worker testWorker = workerPool.makeNewWorker();
      workerPool.workerDone(testWorker);
      
      assertEquals(1, workerPool.availableWorkers.size());
      
      try {
        Worker returnedWorker = workerPool.getExistingWorker(100);
        assertTrue(returnedWorker == testWorker);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  @SuppressWarnings("deprecation")
  @Test
  public void expireOldWorkersTest() {
    workerPool.setKeepAliveTime(0);
    
    synchronized (workerPool.workersLock) {
      // add an idle worker
      Worker testWorker = workerPool.makeNewWorker();
      workerPool.workerDone(testWorker);
      
      assertEquals(1, workerPool.availableWorkers.size());
      
      TestUtils.blockTillClockAdvances();
      
      synchronized (workerPool.workersLock) {
        workerPool.expireOldWorkers();
      }
      
      // should not have collected yet due to core size == 1
      assertEquals(1, workerPool.availableWorkers.size());
      
      workerPool.allowCoreThreadTimeOut(true);
      
      TestUtils.blockTillClockAdvances();
      
      workerPool.expireOldWorkers();
      
      // verify collected now
      assertEquals(0, workerPool.availableWorkers.size());
    }
  }
  
  @Test
  public void killWorkerTest() {
    final Worker w = workerPool.makeNewWorker();
    workerPool.workerDone(w);
    
    workerPool.killWorker(w);
    assertEquals(0, workerPool.getCurrentPoolSize());
    assertTrue(workerPool.availableWorkers.isEmpty());
    new TestCondition() {
      @Override
      public boolean get() {
        return ! w.thread.isAlive();
      }
    }.blockTillTrue();
  }
  
  @Test
  public void workerDoneTest() {
    workerPool.workerDone(workerPool.makeNewWorker());
    
    assertEquals(1, workerPool.availableWorkers.size());
    
    workerPool.startShutdown();
    workerPool.finishShutdown();
    final Worker w = workerPool.makeNewWorker();
    workerPool.workerDone(w);
    
    assertEquals(0, workerPool.availableWorkers.size());
    new TestCondition() {
      @Override
      public boolean get() {
        return ! w.thread.isAlive();
      }
    }.blockTillTrue();
  }
}
