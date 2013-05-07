package org.threadly.concurrent;

import static org.junit.Assert.*;

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
  
  /*@Test
  public void getAndSetCorePoolSizeFail() {
    // TODO
  }*/
  
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
  
  /*@Test
  public void getAndSetMaxPoolSizeFail() {
    // TODO
  }*/
  
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
  
  /*@Test
  public void setKeepAliveTimeFail() {
    // TODO
  }*/
  
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
