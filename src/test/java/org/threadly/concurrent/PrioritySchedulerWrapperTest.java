package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerDefaultPriorityWrapperTest.TestPriorityScheduler;

@SuppressWarnings({"javadoc", "deprecation"})
public class PrioritySchedulerWrapperTest {
  private static PriorityScheduler scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduler(2);
  }
  
  @AfterClass
  public static void cleanupClass() {
    scheduler.shutdown();
    scheduler = null;
  }
  
  @SuppressWarnings("unused")
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
    
    TestPriorityScheduler tps = new TestPriorityScheduler();
    psw = new PrioritySchedulerWrapper(tps, TaskPriority.Low);
    psw.isShutdown();
    
    assertTrue(tps.isShutdownCalled);
  }
  
  @Test
  public void executeTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.execute(DoNothingRunnable.instance());
    assertTrue(testScheduler.executeCalled);
    
    // reset and try with priority
    testScheduler.executeCalled = false;
    psw.execute(DoNothingRunnable.instance(), TaskPriority.High);
    assertTrue(testScheduler.executeCalled);
  }
  
  @Test
  public void scheduleTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.schedule(DoNothingRunnable.instance(), 10);
    assertTrue(testScheduler.scheduleCalled);
    
    // reset and try with priority
    testScheduler.scheduleCalled = false;
    psw.schedule(DoNothingRunnable.instance(), 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleCalled);
  }
  
  @Test
  public void submitRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submit(DoNothingRunnable.instance());
    assertTrue(testScheduler.submitRunnableCalled);
    
    // reset and try with priority
    testScheduler.submitRunnableCalled = false;
    psw.submit(DoNothingRunnable.instance(), TaskPriority.High);
    assertTrue(testScheduler.submitRunnableCalled);
  }
  
  @Test
  public void submitRunnableWithResultTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submit(DoNothingRunnable.instance(), new Object());
    assertTrue(testScheduler.submitRunnableResultCalled);
    
    // reset and try with priority
    testScheduler.submitRunnableResultCalled = false;
    psw.submit(DoNothingRunnable.instance(), new Object(), TaskPriority.High);
    assertTrue(testScheduler.submitRunnableResultCalled);
  }
  
  @Test
  public void submitCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submit(new TestCallable());
    assertTrue(testScheduler.submitCallableCalled);
    
    // reset and try with priority
    testScheduler.submitCallableCalled = false;
    psw.submit(new TestCallable(), TaskPriority.High);
    assertTrue(testScheduler.submitCallableCalled);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(DoNothingRunnable.instance(), 10);
    assertTrue(testScheduler.submitScheduledRunnableCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledRunnableCalled = false;
    psw.submitScheduled(DoNothingRunnable.instance(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledRunnableCalled);
  }
  
  @Test
  public void submitScheduledRunnableWithResultTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(DoNothingRunnable.instance(), new Object(), 10);
    assertTrue(testScheduler.submitScheduledRunnableResultCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledRunnableResultCalled = false;
    psw.submitScheduled(DoNothingRunnable.instance(), new Object(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledRunnableResultCalled);
  }
  
  @Test
  public void submitScheduledCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.submitScheduled(new TestCallable(), 10);
    assertTrue(testScheduler.submitScheduledCallableCalled);
    
    // reset and try with priority
    testScheduler.submitScheduledCallableCalled = false;
    psw.submitScheduled(new TestCallable(), 10, TaskPriority.High);
    assertTrue(testScheduler.submitScheduledCallableCalled);
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 10);
    assertTrue(testScheduler.scheduleWithFixedDelayCalled);
    
    // reset and try with priority
    testScheduler.scheduleWithFixedDelayCalled = false;
    psw.scheduleWithFixedDelay(DoNothingRunnable.instance(), 10, 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleWithFixedDelayCalled);
  }
  
  @Test
  public void scheduleAtFixedRateTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    psw.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 10);
    assertTrue(testScheduler.scheduleAtFixedRateCalled);
    
    // reset and try with priority
    testScheduler.scheduleAtFixedRateCalled = false;
    psw.scheduleAtFixedRate(DoNothingRunnable.instance(), 10, 10, TaskPriority.High);
    assertTrue(testScheduler.scheduleAtFixedRateCalled);
  }
  
  @Test
  public void removeRunnableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    
    psw.remove(DoNothingRunnable.instance());
    
    assertTrue(testScheduler.removeRunnableCalled);
  }
  
  @Test
  public void removeCallableTest() {
    TestPriorityScheduler testScheduler = new TestPriorityScheduler();
    PrioritySchedulerWrapper psw = new PrioritySchedulerWrapper(testScheduler, TaskPriority.Low);
    
    psw.remove(new TestCallable());
    
    assertTrue(testScheduler.removeCallableCalled);
  }
}
