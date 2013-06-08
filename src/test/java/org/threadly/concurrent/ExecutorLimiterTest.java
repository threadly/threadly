package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorLimiterTest {
  private static final int PARALLEL_COUNT = 2;
  private static final int THREAD_COUNT = PARALLEL_COUNT * 2;
  
  private ExecutorService executor;
  private ExecutorLimiter limiter;
  
  @Before
  public void setup() {
    executor = Executors.newFixedThreadPool(THREAD_COUNT);
    limiter = new ExecutorLimiter(executor, PARALLEL_COUNT);
  }
  
  @After
  public void tearDown() {
    executor.shutdown();
    executor = null;
    limiter = null;
  }
  
  @Test
  public void executeTest() {
    int runnableCount = 10;
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      limiter.execute(tr);
      runnables.add(tr);
    }
    
    // TODO - verify that only PARALLEL_COUNT ran in parallel
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished();
      
      assertEquals(tr.getRunCount(), 1);
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    limiter.execute(null);
    fail("Execption should have thrown");
  }
}
