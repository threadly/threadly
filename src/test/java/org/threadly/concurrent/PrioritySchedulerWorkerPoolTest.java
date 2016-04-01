package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.AbstractPriorityScheduler.QueueManager;
import org.threadly.concurrent.PriorityScheduler.Worker;
import org.threadly.concurrent.PriorityScheduler.WorkerPool;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class PrioritySchedulerWorkerPoolTest {
  protected QueueManager qm;
  protected WorkerPool workerPool;
  
  @Before
  public void setup() {
    workerPool = new WorkerPool(new ConfigurableThreadFactory(), 1);
    qm = new QueueManager(workerPool, 1000);
    
    workerPool.start(qm);
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
  public void setPoolSizeSmallerTest() {
    workerPool.setPoolSize(10);
    workerPool.prestartAllThreads();
    
    workerPool.setPoolSize(1);
      
    assertEquals(1, workerPool.getMaxPoolSize());
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
  public void prestartAllThreadsTest() {
    int corePoolSize = 5;
    workerPool.setPoolSize(corePoolSize);
    
    // there must always be at least one thread
    assertEquals(1, workerPool.getCurrentPoolSize());
    
    workerPool.prestartAllThreads();
    
    assertEquals(corePoolSize, workerPool.getCurrentPoolSize());
  }
  
  @Test
  public void workerIdleTest() {
    final Worker w = new Worker(workerPool, workerPool.threadFactory);
    w.start();

    // wait for worker to become idle
    new TestCondition() {
      @Override
      public boolean get() {
        return workerPool.idleWorker.get() == w;
      }
    }.blockTillTrue();
    
    workerPool.startShutdown();
    workerPool.finishShutdown();
    
    // verify idle worker is gone
    new TestCondition() {
      @Override
      public boolean get() {
        return workerPool.idleWorker.get() == null;
      }
    }.blockTillTrue();
    
    // should return immediately now that we are shut down
    workerPool.workerIdle(new Worker(workerPool, workerPool.threadFactory));
  }
}
