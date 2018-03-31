package org.threadly.concurrent.wrapper.limiter;

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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.PrioritySchedulerTest.PrioritySchedulerFactory;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.SubmitterExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestCondition;
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
  
  @Before
  public void setup() {
    new TestCondition(() -> scheduler.getQueuedTaskCount() == 0).blockTillTrue();
  }
  
  protected ExecutorLimiter getLimiter(int parallelCount, boolean limitFutureListenersExecution) {
    return new ExecutorLimiter(scheduler, parallelCount, limitFutureListenersExecution);
  }
  
  @Override
  protected SubmitterExecutorFactory getSubmitterExecutorFactory() {
    return new ExecutorLimiterFactory();
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
  public void getAndSetMaxConcurrencyTest() {
    ExecutorLimiter limiter = getLimiter(PARALLEL_COUNT, true);
    assertEquals(PARALLEL_COUNT, limiter.getMaxConcurrency());
    limiter.setMaxConcurrency(1);
    assertEquals(1, limiter.getMaxConcurrency());
  }
  
  @Test
  public void increaseMaxConcurrencyTest() {
    ExecutorLimiter limiter = getLimiter(1, true);

    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      limiter.execute(btr);
      // block till started so that our entire limit is used up
      btr.blockTillStarted();
      
      TestRunnable tr = new TestRunnable();
      limiter.execute(tr);  // wont be able to run
      
      limiter.setMaxConcurrency(2);
      
      tr.blockTillFinished();  // should be able to complete now that limit was increased
    } finally {
      btr.unblock();
    }
  }
  
  @Test
  public void getUnsubmittedTaskCountTest() {
    ExecutorLimiter limiter = getLimiter(1, true);
    
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
  public void consumeAvailableTest() {
    ExecutorLimiter limiter = getLimiter(PARALLEL_COUNT, true);
    List<TestRunnable> runnables = new ArrayList<>(PARALLEL_COUNT);
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
    Executor limitedExecutor = getLimiter(PARALLEL_COUNT, true);
    final AtomicInteger running = new AtomicInteger(0);
    final AsyncVerifier verifier = new AsyncVerifier();
    List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
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
    ExecutorLimiterFactory elf = new ExecutorLimiterFactory();
    
    try {
      ExecutorLimiter el = elf.makeSubmitterExecutor(TEST_QTY, false);
      
      List<TestRunnable> runnables = new ArrayList<>(TEST_QTY);
      List<Future<?>> futures = new ArrayList<>(TEST_QTY);
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
  
  @Test
  public void futureListenerUnlimitedTest() {
    ExecutorLimiter limiter = getLimiter(1, false);
    BlockingTestRunnable btr = new BlockingTestRunnable();
    try {
      TestRunnable secondTask = new TestRunnable();
      
      limiter.submit(DoNothingRunnable.instance())
             .addListener(btr);
      btr.blockTillStarted();
      limiter.execute(secondTask);
      
      secondTask.blockTillFinished(); // will throw if could not be executed due to listener blocking
    } finally {
      btr.unblock();
    }
  }

  protected static class ExecutorLimiterFactory implements SubmitterExecutorFactory {
    private final PrioritySchedulerFactory schedulerFactory;
    
    protected ExecutorLimiterFactory() {
      schedulerFactory = new PrioritySchedulerFactory();
    }
    
    @Override
    public ExecutorLimiter makeSubmitterExecutor(int poolSize, boolean prestartIfAvailable) {
      SubmitterExecutor executor = schedulerFactory.makeSubmitterExecutor(poolSize * 2, prestartIfAvailable);
      
      return new ExecutorLimiter(executor, poolSize);
    }
    
    @Override
    public void shutdown() {
      schedulerFactory.shutdown();
    }
  }
}
