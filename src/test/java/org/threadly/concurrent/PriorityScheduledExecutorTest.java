package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.NoSuchElementException;

import org.junit.Test;
import org.threadly.test.TestUtil;

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
    
    scheduler.execute(new TestRunnable());
    
    TestUtil.sleep(100);  // wait for execution
    
    assertEquals(scheduler.getCurrentPoolSize(), 1);
  }
  
  private class TestRunnable implements Runnable {
    private int ranCount = 0;
    
    @Override
    public void run() {
      ranCount++;
    }
  }
}
