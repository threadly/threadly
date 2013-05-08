package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.threadly.test.TestCondition;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorTest {
  @Test
  public void getDefaultPriorityTest() {
    TaskPriority priority = TaskPriority.High;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000, 
                                                                        priority, 1000);
    assertEquals(scheduler.getDefaultPriority(), priority);
    scheduler.shutdown();
    
    priority = TaskPriority.Low;
    scheduler = new PriorityScheduledExecutor(1, 1, 1000, 
                                              priority, 1000);
    assertEquals(scheduler.getDefaultPriority(), priority);
    scheduler.shutdown();
  }
  
  @Test
  public void getAndSetCorePoolSizeTest() {
    int corePoolSize = 1;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(corePoolSize, 
                                                                        corePoolSize + 10, 1000);
    
    assertEquals(scheduler.getCorePoolSize(), corePoolSize);
    
    corePoolSize = 10;
    scheduler.setMaxPoolSize(corePoolSize + 10);
    scheduler.setCorePoolSize(corePoolSize);
    
    assertEquals(scheduler.getCorePoolSize(), corePoolSize);
    
    scheduler.shutdown();
  }
  
  @Test
  public void setCorePoolSizeFail() {
    int corePoolSize = 1;
    int maxPoolSize = 10;
    // first construct a valid scheduler
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(corePoolSize, 
                                                                        maxPoolSize, 1000);
    
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
    
    scheduler.shutdown();
  }
  
  @Test
  public void getAndSetMaxPoolSizeTest() {
    int maxPoolSize = 1;
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, maxPoolSize, 1000);
    
    assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
    
    maxPoolSize = 10;
    scheduler.setMaxPoolSize(maxPoolSize);
    
    assertEquals(scheduler.getMaxPoolSize(), maxPoolSize);
    
    scheduler.shutdown();
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
    assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
    
    keepAliveTime = Long.MAX_VALUE;
    scheduler.setKeepAliveTime(keepAliveTime);
    
    assertEquals(scheduler.getKeepAliveTime(), keepAliveTime);
    
    scheduler.shutdown();
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
    // verify nothing at the start
    assertEquals(scheduler.getCurrentPoolSize(), 0);
    
    TestRunnable tr = new TestRunnable();
    scheduler.execute(tr);
    
    tr.blockTillRun();  // wait for execution
    
    assertEquals(scheduler.getCurrentPoolSize(), 1);
    
    scheduler.shutdown();
  }
  
  @Test
  public void executionTest() {
    int runnableCount = 10;
    
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(runnableCount, runnableCount, 1000);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.execute(tr);
      runnables.add(tr);
    }
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillRun();
      
      assertEquals(tr.ranCount, 1);
    }
    
    // run one more time now that all workers are already running
    it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // verify second execution
    it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillRun(2);
      
      assertEquals(tr.ranCount, 2);
    }
    
    scheduler.shutdown();
  }
  
  @Test
  public void scheduleExecutionTest() {
    int runnableCount = 10;
    int scheduleDelay = 200;
    
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(runnableCount, runnableCount, 1000);

    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.schedule(tr, scheduleDelay);
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      long executionDelay = tr.executionDelay();
      assertTrue(executionDelay >= scheduleDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (scheduleDelay + 100));  
      assertEquals(tr.ranCount, 1);
    }
    
    scheduler.shutdown();
  }
  
  private class TestRunnable extends TestCondition implements Runnable {
    private final long createTime;
    private long lastRunTime;
    private int expectedRunCount;
    private int ranCount;
    
    private TestRunnable() {
      createTime = System.currentTimeMillis();
      lastRunTime = -1;
      expectedRunCount = 1;
      ranCount = 0;
    }
    
    public long executionDelay() {
      blockTillRun();
      return lastRunTime - createTime;
    }
    
    @Override
    public void run() {
      lastRunTime = System.currentTimeMillis();
      ranCount++;
    }

    @Override
    public boolean get() {
      return ranCount >= expectedRunCount;
    }
    
    private void blockTillRun() {
      blockTillRun(1);
    }
    
    private void blockTillRun(int totalCount) {
      expectedRunCount = totalCount;
      blockTillTrue();
    }
  }
}
