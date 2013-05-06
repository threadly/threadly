package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    scheduler.tick();
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().ranCount, 1);
    }
    
    // verify no more run after a second tick
    scheduler.tick();
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().ranCount, 1);
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
    scheduler.tick(startTime);

    assertEquals(executeRun.ranCount, 1);   // should have run
    assertEquals(scheduleRun.ranCount, 0);  // should NOT have run yet
    
    scheduler.tick(startTime + scheduleDelay);
    assertEquals(executeRun.ranCount, 1);   // should NOT have run again
    assertEquals(scheduleRun.ranCount, 1);  // should have run
  }
  
  @Test
  public void testRecurring() {
    long delay = 1000 * 10;
    
    TestRunnable immediateRun = new TestRunnable();
    TestRunnable initialDelay = new TestRunnable();
    
    scheduler.scheduleWithFixedDelay(immediateRun, 0, delay);
    scheduler.scheduleWithFixedDelay(initialDelay, delay, delay);

    long startTime = System.currentTimeMillis();
    scheduler.tick(startTime);
    
    assertEquals(immediateRun.ranCount, 1);   // should have run
    assertEquals(initialDelay.ranCount, 0);  // should NOT have run yet

    scheduler.tick(startTime + delay);
    assertEquals(immediateRun.ranCount, 2);   // should have run again
    assertEquals(initialDelay.ranCount, 1);  // should have run for the first time
    
    scheduler.tick(startTime + (delay * 2));
    assertEquals(immediateRun.ranCount, 3);   // should have run again
    assertEquals(initialDelay.ranCount, 2);  // should have run for the first time
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
    scheduler.tick(startTime);
    
    assertEquals(immediateRun.ranCount, 1);   // should have run
    assertEquals(initialDelay.ranCount, 0);  // should NOT have run yet
    
    assertTrue(scheduler.remove(immediateRun));
    
    scheduler.tick(startTime + delay);
    
    assertEquals(immediateRun.ranCount, 1);   // should NOT have run again
    assertEquals(initialDelay.ranCount, 1);  // should have run
  }
  
  private class TestRunnable implements Runnable {
    private int ranCount = 0;
    
    @Override
    public void run() {
      ranCount++;
    }
  }
}
