package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class PrioritySchedulerWrapperTest {
  private static PriorityScheduledExecutor scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduledExecutor(1, 2, 1000);
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduler.shutdown();
    scheduler = null;
  }
  
  @Test
  public void constructorTest() {
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(scheduler, TaskPriority.Low);
    assertTrue(psw.scheduler == scheduler);
    assertEquals(TaskPriority.Low, psw.defaultPriority);
    psw = new PrioritySchedulerWrapper(scheduler, TaskPriority.High);
    assertEquals(TaskPriority.High, psw.defaultPriority);
  }
  
  @Test
  public void constructorFail() {
    try {
      new PrioritySchedulerWrapper(null, TaskPriority.High);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new PrioritySchedulerWrapper(scheduler, null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void isShutdownTest() {
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(scheduler, TaskPriority.Low);
    assertEquals(scheduler.isShutdown(), psw.isShutdown());
  }
}
