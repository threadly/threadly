package org.threadly.concurrent.wrapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.KDCallable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.KDRunnable;
import org.threadly.concurrent.wrapper.KeyDistributedExecutorTest.ThreadContainer;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;

@SuppressWarnings("javadoc")
public class KeyDistributedSchedulerTest extends ThreadlyTester {
  private static final int PARALLEL_LEVEL = Runtime.getRuntime().availableProcessors();
  private static final int RUNNABLE_COUNT_PER_LEVEL = TEST_QTY;
  
  @BeforeAll
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  private PriorityScheduler scheduler;
  private KeyDistributedScheduler distributor;
  
  @BeforeEach
  public void setup() {
    scheduler = new StrictPriorityScheduler(PARALLEL_LEVEL * 2);
    distributor = new KeyDistributedScheduler(scheduler, Integer.MAX_VALUE, false);
  }
  
  @AfterEach
  public void cleanup() {
    scheduler.shutdownNow();
    scheduler = null;
    distributor = null;
  }
  
  private List<KDRunnable> populate(AddHandler ah) {
    final List<KDRunnable> runs = new ArrayList<>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    BlockingTestRunnable[] btrs = new BlockingTestRunnable[PARALLEL_LEVEL];
    for (int i = 0; i < PARALLEL_LEVEL; i++) {
      ThreadContainer tc = new ThreadContainer();
      KDRunnable previous = null;
      btrs[i] = new BlockingTestRunnable();
      distributor.execute(tc, btrs[i]);
      for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
        KDRunnable tr = new KDRunnable(tc, previous);
        runs.add(tr);
        ah.addTDRunnable(tc, tr);
        
        previous = tr;
      }
    }
    for (BlockingTestRunnable btr : btrs) {
      // allow all to execute once submitted / queued, the thread should be consistent at this point
      btr.unblock();
    }
    
    return runs;
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorTest() {
    // none should throw exception
    new KeyDistributedScheduler(scheduler);
    new KeyDistributedScheduler(scheduler, true);
    new KeyDistributedScheduler(scheduler, 1);
    new KeyDistributedScheduler(scheduler, 1, true);
  }
  
  @Test
  @SuppressWarnings("unused")
  public void constructorFail() {
    try {
      new KeyDistributedScheduler(null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getSchedulerForKeyFail() {
      assertThrows(IllegalArgumentException.class, () -> {
      distributor.getSchedulerForKey(null);
      });
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
      tr.blockTillFinished(2_000);
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
    List<KDCallable> runs = new ArrayList<>(PARALLEL_LEVEL * RUNNABLE_COUNT_PER_LEVEL);
    
    BlockingTestRunnable[] btrs = new BlockingTestRunnable[PARALLEL_LEVEL];
    for (int i = 0; i < PARALLEL_LEVEL; i++) {
      ThreadContainer tc = new ThreadContainer();
      KDCallable previous = null;
      btrs[i] = new BlockingTestRunnable();
      distributor.execute(tc, btrs[i]);
      for (int j = 0; j < RUNNABLE_COUNT_PER_LEVEL; j++) {
        KDCallable tr = new KDCallable(tc, previous);
        runs.add(tr);
        distributor.submit(tc, tr);
        
        previous = tr;
      }
    }
    for (BlockingTestRunnable btr : btrs) {
      // allow all to execute once submitted / queued, the thread should be consistent at this point
      btr.unblock();
    }
    
    Iterator<KDCallable> it = runs.iterator();
    while (it.hasNext()) {
      KDCallable tr = it.next();
      tr.blockTillFinished(20_000);
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
      tr.blockTillFinished(2_000);
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
      private int initialDelay = 0;
      
      @Override
      public void addTDRunnable(Object key, KDRunnable tdr) {
        distributor.scheduleTaskWithFixedDelay(key, tdr, initialDelay++, DELAY_TIME);
      }
    });
    
    Iterator<KDRunnable> it = runs.iterator();
    while (it.hasNext()) {
      KDRunnable tr = it.next();
      assertTrue(tr.getDelayTillRun(2) - tr.getDelayTillRun(1) >= DELAY_TIME);
      tr.blockTillFinished(20_000 + (runs.size() * 10) +  (DELAY_TIME * 4), 4);
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
