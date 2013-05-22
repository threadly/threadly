package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SimpleSchedulerInterfaceTest {
  public static void executionTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount);
    
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
      
      assertEquals(tr.getRunCount(), 1);
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
      tr.blockTillRun(1000, 2);
      
      assertEquals(tr.getRunCount(), 2);
    }
  }
  
  public static void executeTestFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1);
    
    scheduler.execute(null);
  }
  
  public static void scheduleExecutionTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    int scheduleDelay = 50;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount);
    
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
      long executionDelay = tr.getDelayTillFirstRun();
      assertTrue(executionDelay >= scheduleDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (scheduleDelay + 200));  
      assertEquals(tr.getRunCount(), 1);
    }
  }
  
  public static void scheduleExecutionFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1);
    try {
      scheduler.schedule(null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.schedule(new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public static void recurringExecutionTest(PrioritySchedulerFactory factory) {
    int runnableCount = 10;
    int recurringDelay = 50;
    
    SimpleSchedulerInterface scheduler = factory.make(runnableCount);

    long startTime = System.currentTimeMillis();
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      scheduler.scheduleWithFixedDelay(tr, 0, recurringDelay);
      runnables.add(tr);
    }
    
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillRun(runnableCount * recurringDelay + 500, 2);
      long executionDelay = tr.getDelayTillRun(2);
      assertTrue(executionDelay >= recurringDelay);
      // should be very timely with a core pool size that matches runnable count
      assertTrue(executionDelay <= (recurringDelay + 500));
      int expectedRunCount = (int)((System.currentTimeMillis() - startTime) / recurringDelay);
      assertTrue(tr.getRunCount() >= expectedRunCount - 2);
      assertTrue(tr.getRunCount() <= expectedRunCount + 2);
    }
  }
  
  public static void recurringExecutionFail(PrioritySchedulerFactory factory) {
    SimpleSchedulerInterface scheduler = factory.make(1);
    try {
      scheduler.scheduleWithFixedDelay(null, 1000, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), -1, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 1000, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  public interface PrioritySchedulerFactory {
    public SimpleSchedulerInterface make(int poolSize);
  }
}
