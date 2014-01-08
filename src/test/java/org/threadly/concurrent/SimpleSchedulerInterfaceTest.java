package org.threadly.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SimpleSchedulerInterfaceTest {
  public static void scheduleTest(SimpleSchedulerFactory factory) {
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(runnableCount, true);
      
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
        assertTrue(executionDelay <= (scheduleDelay + 2000));  
        assertEquals(1, tr.getRunCount());
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void scheduleFail(SimpleSchedulerFactory factory) {
    try {
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  public static void recurringExecutionTest(SimpleSchedulerFactory factory) {
    try {
      final int runnableCount = 10;
      final int recurringDelay = 50;
      final int waitCount = 2;
      
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(runnableCount, true);
      
      // schedule a task first in case there are any initial startup actions which may be slow
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 0, 1000 * 10);
  
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
        tr.blockTillFinished((runnableCount * (recurringDelay * waitCount)) + 2000, waitCount);
        long executionDelay = tr.getDelayTillRun(waitCount);
        assertTrue(executionDelay >= recurringDelay * (waitCount - 1));
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (recurringDelay * (waitCount - 1)) + 2000);
      }
    } finally {
      factory.shutdown();
    }
  }
  
  public static void recurringExecutionFail(SimpleSchedulerFactory factory) {
    try {
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(1, false);
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
    } finally {
      factory.shutdown();
    }
  }
  
  
  public interface SimpleSchedulerFactory {
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, 
                                                        boolean prestartIfAvailable);

    public void shutdown();
  }
}
