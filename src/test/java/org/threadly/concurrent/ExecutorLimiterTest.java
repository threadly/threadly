package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorLimiterTest {
  private static final int PARALLEL_COUNT = 2;
  private static final int THREAD_COUNT = PARALLEL_COUNT * 2;
  private static final int THREAD_SLEEP_TIME = 20;
  
  private volatile boolean parallelFailure;
  private AtomicInteger running;
  private ExecutorService executor;
  private ExecutorLimiter limiter;
  
  @Before
  public void setup() {
    parallelFailure = false;
    running = new AtomicInteger(0);
    executor = Executors.newFixedThreadPool(THREAD_COUNT);
    limiter = new ExecutorLimiter(executor, PARALLEL_COUNT);
  }
  
  @After
  public void tearDown() {
    executor.shutdown();
    executor = null;
    limiter = null;
    running = null;
  }
  
  @Test
  public void constructorFail() {
    try {
      new ExecutorLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new ExecutorLimiter(Executors.newSingleThreadExecutor(), 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void executeTest() {
    int runnableCount = 10;
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable() {
        @Override
        public void handleRunStart() throws InterruptedException {
          running.incrementAndGet();
          if (running.get() > PARALLEL_COUNT) {
            parallelFailure = true;
          }
          Thread.sleep(THREAD_SLEEP_TIME);
        }
        
        @Override
        public void handleRunFinish() {
          running.decrementAndGet();
        }
      };
      limiter.execute(tr);
      runnables.add(tr);
    }
    
    assertFalse(parallelFailure);
    
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
