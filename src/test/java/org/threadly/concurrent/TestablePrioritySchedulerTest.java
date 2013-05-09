package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.TestRunnable;

@SuppressWarnings("javadoc")
public class TestablePrioritySchedulerTest {
  private static final int RUNNABLE_COUNT = 10;
  private static final int THREAD_COUNT = 100;
  
  private PriorityScheduledExecutor parentScheduler;
  private TestablePriorityScheduler testScheduler;
  
  @Before
  public void setup() {
    parentScheduler = new PriorityScheduledExecutor(THREAD_COUNT, THREAD_COUNT, 
                                                    1000, TaskPriority.High, 200);
    testScheduler = new TestablePriorityScheduler(parentScheduler);
  }
  
  @After
  public void tearDown() {
    testScheduler = null;
    parentScheduler.shutdown();
    parentScheduler = null;
  }
  
  @Test
  public void executeTest() {
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      testScheduler.execute(tr);
    }
    
    testScheduler.tick(); // should execute all
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
    
    testScheduler.tick(); // should not execute anything
    
    it = runnables.iterator();
    while (it.hasNext()) {
      assertEquals(it.next().getRunCount(), 1);
    }
  }
}
