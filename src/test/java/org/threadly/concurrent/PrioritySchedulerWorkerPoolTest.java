package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PriorityScheduler.Worker;
import org.threadly.concurrent.PriorityScheduler.WorkerPool;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class PrioritySchedulerWorkerPoolTest {
  protected static int DEFAULT_KEEP_ALIVE_TIME = 1000;
  
  protected WorkerPool workerPool;
  
  @Before
  public void setup() {
    workerPool = new WorkerPool(new ConfigurableThreadFactory(), 1, 
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
  
  @Test
  public void getAndSetPoolSizeTest() {
    int corePoolSize = 10;
    workerPool.setPoolSize(corePoolSize);
      
    assertEquals(corePoolSize, workerPool.getMaxPoolSize());
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
