package org.threadly.concurrent.limiter;

import static org.junit.Assert.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;
import org.threadly.concurrent.BlockingTestRunnable;
import org.threadly.concurrent.PriorityScheduledExecutor;
import org.threadly.concurrent.SimpleSchedulerInterface;
import org.threadly.concurrent.SimpleSchedulerInterfaceTest;
import org.threadly.concurrent.SubmitterExecutorInterface;
import org.threadly.concurrent.SubmitterExecutorInterfaceTest;
import org.threadly.concurrent.SubmitterSchedulerInterface;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest;
import org.threadly.concurrent.TaskPriority;
import org.threadly.concurrent.SubmitterSchedulerInterfaceTest.SubmitterSchedulerFactory;
import org.threadly.concurrent.TestCallable;
import org.threadly.concurrent.future.FutureListenableFuture;
import org.threadly.concurrent.limiter.SubmitterSchedulerLimiter;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestablePriorityScheduler;

@SuppressWarnings("javadoc")
public class SubmitterSchedulerLimiterTest {
  @Test
  public void constructorFail() {
    try {
      new SubmitterSchedulerLimiter(null, 100);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 100);
    try {
      new SubmitterSchedulerLimiter(executor, 0);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void constructorEmptySubPoolNameTest() {
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 100);
    try {
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(executor, 1, " ");
      
      assertNull(limiter.subPoolName);
    } finally {
      executor.shutdown();
    }
  }
  
  @Test
  public void consumeAvailableTest() {
    int testQty = 10;
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(1, 1, 10, TaskPriority.High, 100);
    SubmitterSchedulerLimiter psl = new SubmitterSchedulerLimiter(executor, testQty);
    
    boolean flip1 = true;
    boolean flip2 = true;
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(testQty);
    for (int i = 0; i < testQty; i++) {
      
      if (flip1) {
        TestRunnable tr = new TestRunnable();
        runnables.add(tr);
        if (flip2) {
          psl.waitingTasks.add(psl.new RunnableFutureWrapper(tr, 
                                                             new FutureListenableFuture<Object>()));
          flip2 = false;
        } else {
          psl.waitingTasks.add(psl.new RunnableFutureWrapper(tr, null));
          flip2 = true;
        }
        flip1 = false;
      } else {
        psl.waitingTasks.add(psl.new CallableFutureWrapper<Object>(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            return new Object();
          }
        }, new FutureListenableFuture<Object>()));
        flip1 = true;
      }
    }
    
    psl.consumeAvailable();
    
    // should be fully consumed
    assertEquals(0, psl.waitingTasks.size());
    
    Iterator<TestRunnable> it = runnables.iterator();
    while (it.hasNext()) {
      it.next().blockTillFinished();  // throws exception if it does not finish
    }
  }
  
  @Test
  public void executeTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void executeNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.executeTest(sf);
  }
  
  @Test
  public void submitRunnableTest() {
    submitRunnableTest(false);
  }
  
  @Test
  public void submitRunnableNamedSubPoolTest() {
    submitRunnableTest(true);
  }
  
  public void submitRunnableTest(boolean nameSubPool) {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(nameSubPool);
    
    try {
      int runnableCount = 10;
      
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(runnableCount, false);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submit(tr);
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
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test
  public void submitCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterExecutorInterfaceTest.submitCallableTest(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitRunnableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitRunnableFail(sf);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void submitCallableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterExecutorInterfaceTest.submitCallableFail(sf);
  }
  
  @Test
  public void submitCallableTestableSchedulerTest() throws InterruptedException, ExecutionException {
    int runnableCount = 10;
    int parallelCount = 2;
    PriorityScheduledExecutor executor = new PriorityScheduledExecutor(parallelCount, runnableCount, 
                                                                       1000 * 10);
    try {
      TestablePriorityScheduler testableScheduler = new TestablePriorityScheduler(executor);
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(executor, parallelCount);
      
      List<TestCallable> callables = new ArrayList<TestCallable>(runnableCount);
      List<Future<Object>> futures = new ArrayList<Future<Object>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestCallable tc = new TestCallable(0);
        Future<Object> future = limiter.submit(tc);
        assertNotNull(future);
        callables.add(tc);
        futures.add(future);
      }
      
      testableScheduler.tick();
      
      // verify execution
      Iterator<TestCallable> it = callables.iterator();
      while (it.hasNext()) {
        TestCallable tc = it.next();
        tc.blockTillTrue();
        
        assertTrue(tc.isDone());
      }
      
      it = callables.iterator();
      Iterator<Future<Object>> futureIt = futures.iterator();
      while (futureIt.hasNext()) {
        Future<Object> future = futureIt.next();
        TestCallable tc = it.next();
  
        assertTrue(tc.getReturnedResult() == future.get());
        assertTrue(future.isDone());
      }
    } finally {
      executor.shutdown();
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void executeTestFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.executeFail(sf);
  }
  
  @Test
  public void scheduleExecutionTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.scheduleTest(sf);
  }
  
  @Test
  public void scheduleExecutionFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.scheduleFail(sf);
  }
  
  @Test
  public void submitScheduledRunnableTest() {
    submitScheduledRunnableTest(false);
  }
  
  @Test
  public void submitScheduledRunnableNamedSubPoolTest() {
    submitScheduledRunnableTest(true);
  }
  
  public void submitScheduledRunnableTest(boolean nameSubPool) {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(nameSubPool);
    // we can't defer to the interface implementation for this check
    try {
      int runnableCount = 10;
      int scheduleDelay = 50;
      
      SubmitterSchedulerInterface scheduler = sf.makeSubmitterScheduler(runnableCount, true);
      
      List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
      List<Future<?>> futures = new ArrayList<Future<?>>(runnableCount);
      for (int i = 0; i < runnableCount; i++) {
        TestRunnable tr = new TestRunnable();
        Future<?> future = scheduler.submitScheduled(tr, scheduleDelay);
        assertNotNull(future);
        runnables.add(tr);
        futures.add(future);
      }
      
      // verify execution and execution times
      Iterator<TestRunnable> it = runnables.iterator();
      while (it.hasNext()) {
        TestRunnable tr = it.next();
        long executionDelay = tr.getDelayTillFirstRun();
        assertTrue(executionDelay >= scheduleDelay);
        // should be very timely with a core pool size that matches runnable count
        assertTrue(executionDelay <= (scheduleDelay + 2000));  
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
    } finally {
      sf.shutdown();
    }
  }
  
  @Test
  public void submitScheduledCallableTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledCallableNamedSubPoolTest() throws InterruptedException, ExecutionException {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableTest(sf);
  }
  
  @Test
  public void submitScheduledRunnableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledRunnableFail(sf);
  }
  
  @Test
  public void submitScheduledCallableFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SubmitterSchedulerInterfaceTest.submitScheduledCallableFail(sf);
  }
  
  @Test
  public void recurringExecutionTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionNamedSubPoolTest() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(true);
    
    SimpleSchedulerInterfaceTest.recurringExecutionTest(sf);
  }
  
  @Test
  public void recurringExecutionFail() {
    SchedulerLimiterFactory sf = new SchedulerLimiterFactory(false);
    
    SimpleSchedulerInterfaceTest.recurringExecutionFail(sf);
  }
  
  @Test
  public void removeRunnableTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);
    try {
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(scheduler, 2);
      
      TestRunnable task = new TestRunnable();
      limiter.schedule(task, 1000 * 10);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeCallableTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(2, 2, 1000);
    try {
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(scheduler, 1);
      
      TestCallable task = new TestCallable();
      limiter.submitScheduled(task, 1000 * 10);
      
      assertFalse(scheduler.remove(new TestCallable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      scheduler.shutdownNow();
    }
  }
  
  @Test
  public void removeBlockedRunnableTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(scheduler, 2);
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
  public void removeBlockedCallableTest() {
    PriorityScheduledExecutor scheduler = new PriorityScheduledExecutor(1, 1, 1000);
    BlockingTestRunnable blockingRunnable = new BlockingTestRunnable();
    try {
      SubmitterSchedulerLimiter limiter = new SubmitterSchedulerLimiter(scheduler, 2);
      scheduler.execute(blockingRunnable);
      scheduler.execute(blockingRunnable);
      blockingRunnable.blockTillStarted();

      TestCallable task = new TestCallable();
      limiter.submit(task);
      
      assertFalse(scheduler.remove(new TestRunnable()));
      
      assertTrue(scheduler.remove(task));
    } finally {
      blockingRunnable.unblock();
      scheduler.shutdownNow();
    }
  }

  private class SchedulerLimiterFactory implements SubmitterSchedulerFactory {
    private final List<PriorityScheduledExecutor> executors;
    private final boolean addSubPoolName;
    
    private SchedulerLimiterFactory(boolean addSubPoolName) {
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
    public SubmitterExecutorInterface makeSubmitterExecutor(int poolSize,
                                                            boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }

    @Override
    public SimpleSchedulerInterface makeSimpleScheduler(int poolSize, 
                                                        boolean prestartIfAvailable) {
      return makeSubmitterScheduler(poolSize, prestartIfAvailable);
    }
    
    @Override
    public SubmitterSchedulerInterface makeSubmitterScheduler(int poolSize, 
                                                              boolean prestartIfAvailable) {
      PriorityScheduledExecutor executor = new PriorityScheduledExecutor(poolSize, poolSize, 
                                                                         1000 * 10);
      if (prestartIfAvailable) {
        executor.prestartAllCoreThreads();
      }
      executors.add(executor);
      
      if (addSubPoolName) {
        return new SubmitterSchedulerLimiter(executor, poolSize, "TestSubPool");
      } else {
        return new SubmitterSchedulerLimiter(executor, poolSize);
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
