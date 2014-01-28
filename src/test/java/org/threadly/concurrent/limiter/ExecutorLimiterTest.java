package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.concurrent.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.StrictPriorityScheduledExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
import org.threadly.concurrent.limiter.ExecutorLimiter;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorLimiterTest {
  private static final int PARALLEL_COUNT = TEST_QTY / 2;
  private static final int THREAD_COUNT = PARALLEL_COUNT * 2;
  
  private static PriorityScheduledExecutor scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduledExecutor(PARALLEL_COUNT, THREAD_COUNT, 1000);
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  private ExecutorLimiter limiter;
  
  @Before
  public void setup() {
    limiter = new ExecutorLimiter(scheduler, PARALLEL_COUNT);
  }
  
  @After
  public void tearDown() {
    limiter = null;
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
  public void getMaxConcurrencyTest() {
    assertEquals(PARALLEL_COUNT, limiter.getMaxConcurrency());
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      ExecutorLimiter limiter = new ExecutorLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void consumeAvailableTest() {
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(PARALLEL_COUNT);
    for (int i = 0; i < PARALLEL_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      limiter.waitingTasks.add(limiter.new LimiterRunnableWrapper(tr));
    }
    
    limiter.consumeAvailable();
    
    // should be fully consumed
    assertEquals(0, limiter.waitingTasks.size());
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      it.next().blockTillFinished();  // throws exception if it does not finish
    }
  }
  
  @Test
  public void executeLimitTest() throws InterruptedException, TimeoutException {
    executeLimitTest(limiter, PARALLEL_COUNT);
  }
  
  public static void executeLimitTest(Executor limitedExecutor, 
                                      final int limiterLimit) throws InterruptedException, 
                                                                     TimeoutException {
    final AtomicInteger running = new AtomicInteger(0);
    final AsyncVerifier verifier = new AsyncVerifier();
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = new TestRunnable(20) {
        @Override
        public void handleRunStart() {
          int runningCount = running.incrementAndGet();
          if (runningCount > limiterLimit) {
            verifier.fail(runningCount + " currently running");
          }
        }
        
        @Override
        public void handleRunFinish() {
          running.decrementAndGet();
          verifier.signalComplete();
        }
      };
      limitedExecutor.execute(tr);
      runnables.add(tr);
    }
    
    verifier.waitForTest(1000 * 10, TEST_QTY);
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished();
      
      assertEquals(1, tr.getRunCount());
    }
  }
  
  @Test
  public void executeTest() {
    ExecutorLimiterFactory sf = new ExecutorLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void executeNamedSubPoolTest() {
    ExecutorLimiterFactory sf = new ExecutorLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.executeTest(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    limiter.execute(null);
    fail("Execption should have thrown");
  }
  
  @Test
  public void removeBlockedRunnableTest() {
    PriorityScheduledExecutor scheduler = new StrictPriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      ExecutorLimiter limiter = new ExecutorLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable);
      scheduler.execute(blockingRunnable);
      blockingRunnable.blockTillStarted();
      
      TestRunnable task = new TestRunnable();
      limiter.execute(task);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      assertTrue(scheduler.remove(task));
    } finally {
      blockingRunnable.unblock();
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitRunnableTest(ef);
  }
  
  @Test
  public void submitRunnableWithResultTest() throws InterruptedException, ExecutionException {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitRunnableWithResultTest(ef);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(ef);
  }
  
  @Test
  public void submitCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(ef);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    ExecutorLimiterFactory ef = new ExecutorLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableFail(ef);
  }

  private class ExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final List<PriorityScheduledExecutor> executors;
    private final boolean addSubPoolName;
    
    private ExecutorLimiterFactory(boolean addSubPoolName) {
      executors = new LinkedList<PriorityScheduledExecutor>();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    public ExecutorLimiter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(poolSize, poolSize, 
                                                                               1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      if (addSubPoolName) {
        return new ExecutorLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new ExecutorLimiter(executor, poolSize);
      }
    }
    
    @Override
    public void shutdown() {
      Iterator<PriorityScheduledExecutor> it = executors.iterator();
      while (it.hasNext()) {
        it.next().shutdownNow();
        it.remove();
      }
    }
  }
}
