package org.threadly.concurrent.wrapper;

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
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.KDCallable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.KDRunnable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.ThreadContainer;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.lock.StripedLock;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class KeyDistributedSchedulerTest {
  private static final int PARALLEL_LEVEL = TEST_QTY;
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY * 2;
  
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
  }
  
  private PriorityScheduler scheduler;
  private Object agentLock;
  private KeyDistributedScheduler distributor;
  
  @Before
  public void setup() {
    scheduler = new StrictPriorityScheduler(PARALLEL_LEVEL * 2);
    StripedLock sLock = new StripedLock(1);
    agentLock = sLock.getLock(null);  // there should be only one lock
    distributor = new KeyDistributedScheduler(scheduler, sLock, 
                                              Integer.MAX_VALUE, false);
  }
  
  @After
  public void cleanup() {
    scheduler.shutdownNow();
    scheduler = null;
    agentLock = null;
    distributor = null;
  }
  
  private List<KDRunnable> populate(AddHandler ah) {
    final List<KDRunnable> runs = new ArrayList<KDRunnable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to prevent execution till ready
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        KDRunnable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          KDRunnable tr = new KDRunnable(tc, previous);
          runs.add(tr);
          ah.addTDRunnable(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    return runs;
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new KeyDistributedScheduler(1, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new KeyDistributedScheduler(scheduler, null, 
                                  Integer.MAX_VALUE, false);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorTest() {
    // none should throw exception
    new KeyDistributedScheduler(scheduler);
    new KeyDistributedScheduler(scheduler, true);
    new KeyDistributedScheduler(scheduler, 1);
    new KeyDistributedScheduler(scheduler, 1, true);
    new KeyDistributedScheduler(1, scheduler);
    new KeyDistributedScheduler(1, scheduler, true);
    new KeyDistributedScheduler(1, scheduler, 1);
    new KeyDistributedScheduler(1, scheduler, 1, true);
    StripedLock sLock = new StripedLock(1);
    new KeyDistributedScheduler(scheduler, sLock, 1, false);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getSchedulerForKeyFail() {
    distributor.getSchedulerForKey(null);
  }
  
  @Test
  public void getExecutorTest() {
    assertTrue(scheduler == distributor.getExecutor());
  }
  
  @Test
  public void executeTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.execute(key, tdr);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void executeFail() {
    try {
      distributor.execute(null, DoNothingRunnable.instance());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      distributor.execute(new Object(), null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitRunnableFail() {
    try {
      distributor.submit(null, DoNothingRunnable.instance());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), null, null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitCallableConsistentThreadTest() {
    List<KDCallable> runs = new ArrayList<KDCallable>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    // hold agent lock to avoid execution till all are submitted
    synchronized (agentLock) {
      for (int i = 0; i < PARALLEL_LEVEL; i++) {
        ThreadContainer tc = new ThreadContainer();
        KDCallable previous = null;
        for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
          KDCallable tr = new KDCallable(tc, previous);
          runs.add(tr);
          distributor.submit(tc, tr);
          
          previous = tr;
        }
      }
    }
    
    Iterator<KDCallable> it = runs.iterator();
    while (it.hasNext()) {
      KDCallable tr = it.next();
      tr.blockTillFinished(20 * 1000);
      assertTrue(tr.threadTracker.threadConsistent());  // verify that all threads for a given key ran in the same thread
      assertTrue(tr.previousRanFirst());  // verify runnables were run in order
    }
  }
  
  @Test
  public void submitCallableFail() {
    try {
      distributor.submit(null, new TestCallable());
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submit(new Object(), (Callable<Void>)null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void scheduleExecutionTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.schedule(key, tdr, DELAY_TIME);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      tr.blockTillFinished(1000);
      assertEquals(1, tr.getRunCount()); // verify each only ran once
      assertTrue(tr.getDelayTillFirstRun() >= DELAY_TIME);
    }
  }
  
  @Test
  public void scheduleExecutionFail() {
    try {
      distributor.schedule(new Object(), null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(new Object(), DoNothingRunnable.instance(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.schedule(null, DoNothingRunnable.instance(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    try {
      distributor.submitScheduled(new Object(), (Runnable)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(new Object(), DoNothingRunnable.instance(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(null, DoNothingRunnable.instance(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void submitScheduledCallableFail() {
    try {
      distributor.submitScheduled(new Object(), (Callable<?>)null, 1000);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(new Object(), new TestCallable(), -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.submitScheduled(null, new TestCallable(), 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void recurringExecutionTest() {
    List<KDRunnable> runs = populate(new AddHandler() {
      int initialDelay = 0;
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.scheduleTaskWithFixedDelay(key, tdr, initialDelay++, DELAY_TIME);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      assertTrue(tr.getDelayTillRun(2) >= DELAY_TIME);
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
      distributor.scheduleTaskWithFixedDelay(new Object(), DoNothingRunnable.instance(), -1, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(new Object(), DoNothingRunnable.instance(), 100, -1);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      distributor.scheduleTaskWithFixedDelay(null, DoNothingRunnable.instance(), 100, 100);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void removeRunnableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    KeyDistributedScheduler distributor = new KeyDistributedScheduler(scheduler);
    TestRunnable scheduleRunnable = new TestRunnable();
    TestRunnable submitScheduledRunnable = new TestRunnable();
    TestRunnable scheduleWithFixedDelayRunnable = new TestRunnable();
    
    distributor.schedule(scheduleRunnable, scheduleRunnable, 10);
    distributor.submitScheduled(submitScheduledRunnable, submitScheduledRunnable, 10);
    distributor.scheduleTaskWithFixedDelay(scheduleWithFixedDelayRunnable, scheduleWithFixedDelayRunnable, 10, 10);
    
    assertTrue(scheduler.remove(scheduleRunnable));
    assertTrue(scheduler.remove(submitScheduledRunnable));
    assertTrue(scheduler.remove(scheduleWithFixedDelayRunnable));
  }
  
  @Test
  public void removeCallableTest() {
    TestableScheduler scheduler = new TestableScheduler();
    KeyDistributedScheduler distributor = new KeyDistributedScheduler(scheduler);
    TestCallable submitScheduledCallable = new TestCallable();
    
    distributor.submitScheduled(submitScheduledCallable, submitScheduledCallable, 10);
    
    assertTrue(scheduler.remove(submitScheduledCallable));
  }
  
  private interface AddHandler {
    public void addTDRunnable(Object key, KDRunnable tdr);
  }
}
