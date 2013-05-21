package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtil;

@SuppressWarnings("javadoc")
public class PriorityScheduledExecutorServiceWrapperTest {
  PriorityScheduledExecutorServiceWrapper wrapper;
  
  @Before
  public void setup() {
    wrapper = new PriorityScheduledExecutorServiceWrapper(new PriorityScheduledExecutor(1000, 1000, 200));
  }
  
  @After
  public void tearDown() {
    wrapper.shutdown();
    wrapper = null;
  }
  
  @Test
  public void isTerminatedTest() {
    assertFalse(wrapper.isTerminated());
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() throws InterruptedException {
        Thread.sleep(50);
      }
    };
    wrapper.execute(tr);
    
    TestUtil.sleep(10);
    wrapper.shutdown();

    tr.blockTillRun();
    TestUtil.sleep(100);
    assertTrue(wrapper.isTerminated());
  }
  
  @Test
  public void awaitTerminationTest() throws InterruptedException {
    final int sleepTime = 50;
    
    assertFalse(wrapper.isTerminated());
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() throws InterruptedException {
        Thread.sleep(sleepTime);
      }
    };
    long start = System.currentTimeMillis();
    wrapper.execute(tr);
    
    TestUtil.sleep(10);
    wrapper.shutdown();

    wrapper.awaitTermination(1000, TimeUnit.MILLISECONDS);
    long stop = System.currentTimeMillis();
    
    assertTrue(stop - start >= sleepTime - 10);
  }
  
  @Test
  public void submitCallableTest() throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable(0);
    Future<Object> f = wrapper.submit(tc);
    
    assertTrue(f.get() == tc.result);
  }
  
  @Test (expected = TimeoutException.class)
  public void futureGetTimeoutFail() throws InterruptedException, ExecutionException, TimeoutException {
    TestCallable tc = new TestCallable(100);
    Future<Object> f = wrapper.submit(tc);
    f.get(1, TimeUnit.MILLISECONDS);
    fail("Exception should have been thrown");
  }
  
  @Test (expected = ExecutionException.class)
  public void futureGetExecutionFail() throws InterruptedException, ExecutionException {
    Future<?> f = wrapper.submit(new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException("fail");
      }
    });
    
    f.get();
    fail("Exception should have been thrown");
  }
  
  @Test
  public void futureCancelTest() throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable(500);
    final Future<Object> f = wrapper.submit(tc);
    
    wrapper.schedule(new Runnable() {
      @Override
      public void run() {
        f.cancel(true);
      }
    }, 10);
    
    try {
      f.get();
      fail("exception should have been thrown");
    } catch (CancellationException e) {
      // expected
    }
    
    assertTrue(f.isCancelled());
  }
  
  @Test
  public void scheduleCallableTest() throws InterruptedException, ExecutionException {
    TestCallable tc = new TestCallable(0);
    ScheduledFuture<Object> f = wrapper.schedule(tc, 0, TimeUnit.MILLISECONDS);
    assertTrue(f.getDelay(TimeUnit.MILLISECONDS) <= 0);
    assertTrue(tc.result == f.get());
    
    assertTrue(f.isDone());
  }
  
  @Test
  public void scheduleCallableCancelTest() {
    TestCallable tcDelay = new TestCallable(0);
    ScheduledFuture<Object> delayF = wrapper.schedule(tcDelay, 20, TimeUnit.MILLISECONDS);
    long delay = delayF.getDelay(TimeUnit.MILLISECONDS);
    delayF.cancel(true);
    
    assertTrue(delay <= 20);
    assertTrue(delayF.isCancelled());
    
    TestUtil.sleep(10); // verify it wont run after we wait past the delay
    
    assertFalse(delayF.isDone());
  }
  
  @Test
  public void scheduleWithFixedDelayTest() {
    int runnableCount = 10;
    int recurringDelay = 50;
    int waitCount = 2;
    
    long startTime = System.currentTimeMillis();
    List<TestRunnable> runnables = new ArrayList<TestRunnable>(runnableCount);
    List<ScheduledFuture<?>> futures = new ArrayList<ScheduledFuture<?>>(runnableCount);
    for (int i = 0; i < runnableCount; i++) {
      TestRunnable tr = new TestRunnable();
      ScheduledFuture<?> sf = wrapper.scheduleWithFixedDelay(tr, 0, recurringDelay, 
                                                             TimeUnit.MILLISECONDS);
      runnables.add(tr);
      futures.add(sf);
    }
      
    // verify execution and execution times
    Iterator<TestRunnable> it = runnables.iterator();
    Iterator<ScheduledFuture<?>> fIt = futures.iterator();
    while (it.hasNext()) {
      // verify runnable
      TestRunnable tr = it.next();
      
      tr.blockTillRun(runnableCount * recurringDelay + 500, waitCount);
      long executionDelay = tr.getDelayTillRun(waitCount);
      assertTrue(executionDelay >= recurringDelay * waitCount);
      
      assertTrue(executionDelay <= ((recurringDelay * waitCount) + 500));
      int expectedRunCount = (int)((System.currentTimeMillis() - startTime) / recurringDelay);
      assertTrue(tr.getRunCount() >= expectedRunCount - 2);
      assertTrue(tr.getRunCount() <= expectedRunCount + 2);

      // verify future
      ScheduledFuture<?> sf = fIt.next();
      assertTrue(sf.isDone());
    }
  }
  
  private class TestCallable extends TestCondition 
                             implements Callable<Object> {
    private final long runTime;
    private final Object result;
    private volatile boolean done;
    
    private TestCallable(long runTime) {
      this.runTime = runTime;
      result = new Object();
      done = false;
    }

    @Override
    public Object call() {
      TestUtil.sleep(runTime);
      
      done = true;
      
      return result;
    }

    @Override
    public boolean get() {
      return done;
    }
  }
}
