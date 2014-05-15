package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.TaskExecutorDistributorTest.TDCallable;
import org.threadly.concurrent.TaskExecutorDistributorTest.TDRunnable;
import org.threadly.concurrent.TaskExecutorDistributorTest.ThreadContainer;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class TaskSchedulerDistributorTest {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  private PriorityScheduler scheduler;
  private Object agentLock;
  private TaskSchedulerDistributor distributor;
  
  @Before
  public void setup() {
    scheduler = new StrictPriorityScheduler(PARALLEL_LEVEL + 1, 
                                                    PARALLEL_LEVEL * 2, 
                                                    1000 * 10, 
                                                    TaskPriority.High, 
                                                    PriorityScheduler.DEFAULT_LOW_PRIORITY_MAX_WAIT_IN_MS);
    StripedLock sLock = new StripedLock(1);
    agentLock = sLock.getLock(null);  // there should be only one lock
    distributor = new TaskSchedulerDistributor(scheduler, sLock, 
                                               Integer.MAX_VALUE, false);
  }
  
  @After
  public void tearDown() {
    scheduler.shutdownNow();
    scheduler = null;
    agentLock = null;
    distributor = null;
  }
  
  private List<TDRunnable> populate(AddHandler ah) {
    final List<TDRunnable> runs = new ArrayList<TDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to prevent execution till ready
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDRunnable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDRunnable tr = new TDRunnable(tc, previous);
          runs.add(tr);
          ah.addTDRunnable(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    return runs;
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorFail() {
    try {
      new TaskSchedulerDistributor(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new TaskSchedulerDistributor(scheduler, null, 
                                   Integer.MAX_VALUE, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @SuppressWarnings("unused")
  @Test
  public void constructorTest() {
    // none should throw exception
    new TaskSchedulerDistributor(scheduler);
    new TaskSchedulerDistributor(scheduler, true);
    new TaskSchedulerDistributor(scheduler, 1);
    new TaskSchedulerDistributor(scheduler, 1, true);
    new TaskSchedulerDistributor(1, scheduler);
    new TaskSchedulerDistributor(1, scheduler, true);
    new TaskSchedulerDistributor(1, scheduler, 1);
    new TaskSchedulerDistributor(1, scheduler, 1, true);
    StripedLock sLock = new StripedLock(1);
    new TaskSchedulerDistributor(scheduler, sLock, 1, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getSubmitterSchedulerForKeyFail() {
    distributor.getSubmitterSchedulerForKey(null);
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void addTaskTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.addTask(key, tdr);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void addTaskFail() {
    try {
      distributor.addTask(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.addTask(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submitTask(null, new TestRunnable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), null, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitCallableConsistentThreadTest() {
    List<TDCallable> runs = new ArrayList<TDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to avoid execution till all are submitted
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        TDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          TDCallable tr = new TDCallable(tc, previous);
          runs.add(tr);
          distributor.submitTask(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    Iterator<TDCallable> it = runs.iterator();
    while (it.hasNext()) {
      TDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submitTask(null, new TestCallable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitTask(new Object(), (Callable<Object>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void scheduleExecutionTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.scheduleTask(key, tdr, SCHEDULE_DELAY);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.getDelayTillFirstRun() >= SCHEDULE_DELAY);
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    try {
      distributor.scheduleTask(new Object(), null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTask(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTask(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    try {
      distributor.submitScheduledTask(new Object(), (Runnable)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(new Object(), new TestRunnable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(null, new TestRunnable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledCallableFail() {
    try {
      distributor.submitScheduledTask(new Object(), (Callable<?>)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(new Object(), new TestCallable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduledTask(null, new TestCallable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    List<TDRunnable> runs = populate(new AddHandler() {
      int initialDelay = 0;
      @Override
      public void addTDRunnable(Object key, TDRunnable tdr) {
        distributor.scheduleTaskWithFixedDelay(key, tdr, initialDelay++, 
                                               SCHEDULE_DELAY);
      }
    });
    
    Iterator<TDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      TDRunnable tr = it.next();
      assertTrue(tr.getDelayTillRun(2) >= SCHEDULE_DELAY);
      tr.blockTillFinished(10 * 1000, 3);
      assertFalse(tr.ranConcurrently());  // verify that it never run in parallel
    }
  }
  
  @Test
  public void recurringExecutionFail() {
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), null, 1000, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), new TestRunnable(), -1, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), new TestRunnable(), 100, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(null, new TestRunnable(), 100, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    TaskSchedulerDistributor distributor = new TaskSchedulerDistributor(scheduler);
    TestRunnable scheduleRunnable = new TestRunnable();
    TestRunnable submitScheduledRunnable = new TestRunnable();
    TestRunnable scheduleWithFixedDelayRunnable = new TestRunnable();
    
    distributor.scheduleTask(scheduleRunnable, scheduleRunnable, 10);
    distributor.submitScheduledTask(submitScheduledRunnable, submitScheduledRunnable, 10);
    distributor.scheduleTaskWithFixedDelay(scheduleWithFixedDelayRunnable, scheduleWithFixedDelayRunnable, 10, 10);
    
    assertTrue(scheduler.remove(scheduleRunnable));
    assertTrue(scheduler.remove(submitScheduledRunnable));
    assertTrue(scheduler.remove(scheduleWithFixedDelayRunnable));
  }
  
  @Test
  public void removeCallableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    TaskSchedulerDistributor distributor = new TaskSchedulerDistributor(scheduler);
    TestCallable submitScheduledCallable = new TestCallable();
    
    distributor.submitScheduledTask(submitScheduledCallable, submitScheduledCallable, 10);
    
    assertTrue(scheduler.remove(submitScheduledCallable));
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, TDRunnable tdr);
  }
}
