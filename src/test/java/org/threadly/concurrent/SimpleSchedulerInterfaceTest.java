package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class SimpleSchedulerInterfaceTest {
  public static void scheduleTest(SimpleSchedulerFactory factory) {
    try {
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(TEST_QTY, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        scheduler.schedule(tr, SCHEDULE_DELAY);
        runnables.add(tr);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= SCHEDULE_DELAY);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (SCHEDULE_DELAY + 2000));  
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
      SimpleSchedulerInterface scheduler = factory.makeSimpleScheduler(TEST_QTY, true);
      
      // schedule a task first in case there are any initial startup actions which may be slow
      scheduler.scheduleWithFixedDelay(new TestRunnable(), 0, 1000 * 10);
  
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        scheduler.scheduleWithFixedDelay(tr, 0, SCHEDULE_DELAY);
        runnables.add(tr);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished((TEST_QTY * (SCHEDULE_DELAY * CYCLE_COUNT)) + 2000, CYCLE_COUNT);
        long executionDelay = tr.getDelayTillRun(CYCLE_COUNT);
        assertTrue(executionDelay >= SCHEDULE_DELAY * (CYCLE_COUNT - 1));
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (SCHEDULE_DELAY * (CYCLE_COUNT - 1)) + 2000);
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
