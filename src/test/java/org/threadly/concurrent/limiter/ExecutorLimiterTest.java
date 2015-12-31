package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.limiter.ExecutorLimiter;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ExecutorLimiterTest extends SubmitterExecutorInterfaceTest {
  protected static final int PARALLEL_COUNT = TEST_QTY / 2;
  protected static final int THREAD_COUNT = PARALLEL_COUNT * 2;
  
  protected static PriorityScheduler scheduler;
  
  @BeforeClass
  public static void setupClass() {
    scheduler = new StrictPriorityScheduler(THREAD_COUNT);
    
    SubmitterExecutorInterfaceTest.setupClass();
  }
  
  @AfterClass
  public static void cleanupClass() {
    scheduler.shutdownNow();
    scheduler = null;
  }
  
  protected ExecutorLimiter getLimiter(int parallelCount) {
    return new ExecutorLimiter(scheduler, parallelCount);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorLimiterFactory(false);
  }
  
  @Test
  @SuppressWarnings("unused")
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
    assertEquals(PARALLEL_COUNT, getLimiter(PARALLEL_COUNT).getMaxConcurrency());
  }
  
  @Test
  public void getUnsubmittedTaskCountTest() {
    ExecutorLimiter limiter = getLimiter(1);
    
    assertEquals(0, limiter.getUnsubmittedTaskCount());
    
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(btr);
      // block till started, and first check should still be zero
      btr.blockTillStarted();
      
      for (int i = 0; i < TEST_QTY; i ++) {
        assertEquals(i, limiter.getUnsubmittedTaskCount());
        limiter.execute(DoNothingRunnable.instance());
      }
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    @SuppressWarnings("deprecation")
    ExecutorLimiter limiter = new ExecutorLimiter(scheduler, 1, "");
    
    assertTrue(limiter.executor == scheduler);
  }
  
  @Test
  public void consumeAvailableTest() {
    ExecutorLimiter limiter = getLimiter(PARALLEL_COUNT);
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(PARALLEL_COUNT);
    for (int i = 0; i < PARALLEL_COUNT; i++) {
      TestRunnable tr = new TestRunnable();
      runnables.add(tr);
      limiter.waitingTasks.add(limiter.new LimiterRunnableWrapper(limiter.executor, tr));
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
    Executor limitedExecutor = getLimiter(PARALLEL_COUNT);
    final AtomicInteger running = new AtomicInteger(0);
    final AsyncVerifier verifier = new AsyncVerifier();
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = new TestRunnable(20) {
        @Override
        public void handleRunStart() {
          int runningCount = running.incrementAndGet();
          if (runningCount > PARALLEL_COUNT) {
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
  
  @Override
  @Test
  public void submitRunnableTest() throws InterruptedException, ExecutionException {
    submitRunnableTest(false);
  }
  
  protected void submitRunnableTest(boolean nameSubPool) throws InterruptedException, ExecutionException {
    ExecutorLimiterFactory elf = new ExecutorLimiterFactory(nameSubPool);
    
    try {
      ExecutorLimiter el = elf.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<Future<?>>(TEST_QTY);
      for (int i = 0; i < TEST_QTY; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = el.submit(tr);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        tr.blockTillFinished();
        
        assertEquals(1, tr.getRunCount());
      }
      
      Iterator<Future<?>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<?> f = futureIt.next();
        try {
          f.get();
        } catch (InterruptedException e) {
          fail();
        } catch (ExecutionException e) {
          fail();
        }
        assertTrue(f.isDone());
      }
      
      super.submitRunnableTest();
    } finally {
      elf.shutdown();
    }
  }

  protected static class ExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    private final boolean addSubPoolName;
    
    protected ExecutorLimiterFactory(boolean addSubPoolName) {
      schedulerFactory = new PrioritySchedulerFactory();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public ExecutorLimiter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize * 2, prestartIfAvailable);
      
      if (addSubPoolName) {
        return new ExecutorLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new ExecutorLimiter(executor, poolSize);
      }
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
