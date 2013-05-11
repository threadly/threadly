package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class NoThreadSchedulerTest {
  private static final int TEST_QTY = 10;
  
  private NoThreadScheduler scheduler;
  
  @Before
  public void setup() {
    scheduler = new NoThreadScheduler();
  }
  
  @After
  public void tearDown() {
    scheduler = null;
  }
  
  private List<TestRunnable> getRunnableList() {
    List<TestRunnable> result = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      result.add(new TestRunnable());
    }
    
    return result;
  }
  
  @Test
  public void testExecute() {
    List<TestRunnable> runnables = getRunnableList();
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      scheduler.execute(it.next());
    }
    
    // all should run now
    assertEquals(scheduler.tick(), TEST_QTY);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    // verify no more run after a second tick
    assertEquals(scheduler.tick(), 0);
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
  }
  
  @Test
  public void testSchedule() {
    long scheduleDelay = 1000 * 10;
    
    TestRunnable executeRun = new TestRunnable();
    TestRunnable scheduleRun = new TestRunnable();
    
    scheduler.schedule(scheduleRun, scheduleDelay);
    scheduler.execute(executeRun);

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);

    assertEquals(executeRun.getRunCount(), 1);   // should have run
    assertEquals(scheduleRun.getRunCount(), 0);  // should NOT have run yet
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 1);
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should have run
    
    assertEquals(scheduler.tick(startTime + scheduleDelay), 0); // should not execute anything
    
    assertEquals(executeRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(scheduleRun.getRunCount(), 1);  // should NOT have run again
  }
  
  @Test
  public void testRecurring() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);  // should have run
    assertEquals(initialDelay.getRunCount(), 0);  // should NOT have run yet

    assertEquals(scheduler.tick(startTime + delay), 2);
    
    assertEquals(immediateRun.getRunCount(), 2);  // should have run again
    assertEquals(initialDelay.getRunCount(), 1);  // should have run for the first time
    
    assertEquals(scheduler.tick(startTime + (delay * 2)), 2);
    
    assertEquals(immediateRun.getRunCount(), 3);  // should have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should have run again
    
    assertEquals(scheduler.tick(startTime + (delay * 2)), 0); // should not execute anything
    
    assertEquals(immediateRun.getRunCount(), 3);  // should NOT have run again
    assertEquals(initialDelay.getRunCount(), 2);  // should NOT have run again
  }
  
  @Test
  public void testRemoval() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    assertFalse(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);
    
    long startTime = System.currentTimeMillis();
    assertEquals(scheduler.tick(startTime), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);   // should have run
    assertEquals(initialDelay.getRunCount(), 0);  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    assertEquals(scheduler.tick(startTime + delay), 1);
    
    assertEquals(immediateRun.getRunCount(), 1);   // should NOT have run again
    assertEquals(initialDelay.getRunCount(), 1);  // should have run
    
    assertEquals(scheduler.tick(startTime + delay), 0); // should not execute anything
    
    assertEquals(immediateRun.getRunCount(), 1);   // should NOT have run
    assertEquals(initialDelay.getRunCount(), 1);  // should NOT have run
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void tickFail() {
    long now;
    scheduler.tick(now = System.currentTimeMillis());
    
    scheduler.tick(now - 1);
    fail("Exception should have been thrown");
  }
}
