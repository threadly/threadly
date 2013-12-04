package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest.SubmitterExecutorFactory;
import org.threadly.concurrent.limiter.ExecutorLimiter;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestablePriorityScheduler;

@SuppressWarnings("javadoc")
public class ExecutorLimiterTest {
  private static final int PARALLEL_COUNT = 2;
  private static final int THREAD_COUNT = PARALLEL_COUNT * 2;
  private static final int THREAD_SLEEP_TIME = 20;
  
  private volatile boolean parallelFailure;
  private AtomicInteger running;
  private PriorityScheduledExecutor executor;
  private ExecutorLimiter limiter;
  
  @Before
  public void setup() {
    parallelFailure = false;
    running = new AtomicInteger(0);
    executor = new PriorityScheduledExecutor(PARALLEL_COUNT, THREAD_COUNT, 1000);
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
  public void getMaxConcurrencyTest() {
    assertEquals(PARALLEL_COUNT, limiter.getMaxConcurrency());
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 100);
    try {
      ExecutorLimiter limiter = new ExecutorLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdown();
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
  public void executeTest() {
    int runnableCount = 10;
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable(THREAD_SLEEP_TIME) {
        @Override
        protected void handleRunStart() {
          running.incrementAndGet();
          if (running.get() > PARALLEL_COUNT) {
            parallelFailure = true;
          }
        }
        
        @Override
        protected void handleRunFinish() {
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
      
      assertEquals(1, tr.getRunCount());
    }
  }
  
  @Test
  public void executeTestableSchedulerTest() {
    int runnableCount = 10;
    
    TestablePriorityScheduler testScheduler = new TestablePriorityScheduler(executor);
    ExecutorLimiter limiter = new ExecutorLimiter(testScheduler, PARALLEL_COUNT);
    
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      limiter.execute(tr);
      runnables.add(tr);
    }
    
    testScheduler.tick();
    
    // verify execution
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      TestRunnable tr = it.next();
      tr.blockTillFinished();
      
      assertEquals(1, tr.getRunCount());
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeFail() {
    limiter.execute(null);
    fail("Execption should have thrown");
  }
  
  @Test
  public void removeBlockedRunnableTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
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
      Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          // ignored
        }
      });
      
      executors = new LinkedList<PriorityScheduledExecutor>();
      this.addSubPoolName = addSubPoolName;
    }
    
    @Override
    public ExecutorLimiter make(int poolSize, boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new PriorityScheduledExecutor(poolSize, poolSize, 
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
        it.next().shutdown();
        it.remove();
      }
    }
  }
}
