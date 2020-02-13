package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class CentralThreadlyPoolTest extends ThreadlyTester {
  @Test
  public void getAndIncreaseGenericThreadsTest() {
    int startingCount = CentralThreadlyPool.getGenericThreadCount();
    assertTrue(startingCount >= 1);
    CentralThreadlyPool.increaseGenericThreads(1);
    assertEquals(startingCount + 1, CentralThreadlyPool.getGenericThreadCount());
  }
  
  private static void verifyGuaranteedThreadProtection(List<SchedulerService> executors, int expectedLimit) {
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<>(expectedLimit);
    try {
      for (SchedulerService executor : executors) {
        for (int i = 0; i < expectedLimit; i++) {
          BlockingTestRunnable btr = new BlockingTestRunnable();
          blockingRunnables.add(btr);
          executor.execute(btr);
        }
        for (BlockingTestRunnable btr : blockingRunnables) {
          btr.blockTillStarted();
        }
        // verify additional tasks would queue
        int startingQueueCount = executor.getQueuedTaskCount();
        executor.execute(DoNothingRunnable.instance());
        TestUtils.sleep(DELAY_TIME);
        assertEquals(startingQueueCount + 1, executor.getQueuedTaskCount());
        
        // verify we can still execute on pool with existing threads
        TestRunnable tr = new TestRunnable();
        CentralThreadlyPool.lowPriorityPool().execute(tr);
        tr.blockTillStarted();
      }
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
  }
  
  @Test
  public void computationPoolTest() {
    verifyGuaranteedThreadProtection(Collections.singletonList(CentralThreadlyPool.computationPool()), 
                                     Runtime.getRuntime().availableProcessors());
  }
  
  @Test
  public void computationPoolThreadRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.computationPool(threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test
  public void rangedThreadPoolAvailableExecuteTest() throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.rangedThreadPool(0, 1).execute(av::signalComplete);
    av.waitForTest();
  }
  
  @Test
  public void rangedThreadPoolThreadRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.rangedThreadPool(0, 1, threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void rangedThreadPoolNullPriorityFail() {
    CentralThreadlyPool.rangedThreadPool((TaskPriority)null, 0, 2, null);
  }
  
  @Test
  public void lowPrioritySingleThreadExecuteTest() throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.lowPrioritySingleThreadPool().execute(av::signalComplete);
    av.waitForTest();
  }
  
  @Test
  public void lowPrioritySingleThreadRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.lowPrioritySingleThreadPool(threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test
  public void lowPriorityRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.lowPriorityPool(threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test
  public void singleThreadPoolsGuaranteedThreadTest() {
    List<SchedulerService> executors = new ArrayList<>();
    for (int i = 0; i < TEST_QTY * 2; i++) {
      executors.add(CentralThreadlyPool.singleThreadPool());
    }
    verifyGuaranteedThreadProtection(executors, 1);
  }
  
  @Test
  public void singleThreadPoolSingleThreadedTest() {
    final int runCount = TEST_QTY * 20;
    PrioritySchedulerService singleThreadedPool = CentralThreadlyPool.singleThreadPool(false);
    TestRunnable tr = new TestRunnable();
    for (int i = 0; i < runCount; i++) {
      singleThreadedPool.execute(tr);
    }
    tr.blockTillFinished(20_000, runCount);
    assertFalse(tr.ranConcurrently());
  }
  
  @Test
  public void singleThreadRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.singleThreadPool(false, threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test
  public void singleThreadPriorityTest() throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.singleThreadPool(false, null, Thread.MIN_PRIORITY).execute(() -> {
      av.assertEquals(Thread.MIN_PRIORITY, Thread.currentThread().getPriority());
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test
  public void threadPoolsGuaranteedThreadTest() {
    int threadsPerScheduler = 10;
    List<SchedulerService> executors = new ArrayList<>();
    for (int i = 0; i < TEST_QTY * 2; i++) {
      executors.add(CentralThreadlyPool.threadPool(threadsPerScheduler));
    }
    verifyGuaranteedThreadProtection(executors, threadsPerScheduler);
  }
  
  @Test
  public void threadPoolRenamedTest() throws InterruptedException, TimeoutException {
    final String threadName = StringUtils.makeRandomString(5);
    AsyncVerifier av = new AsyncVerifier();
    CentralThreadlyPool.threadPool(2, threadName).execute(() -> {
      av.assertTrue(Thread.currentThread().getName().startsWith(threadName));
      av.signalComplete();
    });
    av.waitForTest();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void threadPoolNullPriorityFail() {
    CentralThreadlyPool.threadPool((TaskPriority)null, 2, null);
  }
  
  @Test
  public void isolatedPoolTest() {
    int testQty = 100;
    List<BlockingTestRunnable> blockingRunnables = new ArrayList<>(testQty);
    try {
      SubmitterExecutor executor = CentralThreadlyPool.isolatedTaskPool();
      for (int i = 0; i < testQty; i++) {
        BlockingTestRunnable btr = new BlockingTestRunnable();
        blockingRunnables.add(btr);
        executor.execute(btr);
      }
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.blockTillStarted();
      }
      // verify we can still execute on pool with existing threads
      TestRunnable tr = new TestRunnable();
      CentralThreadlyPool.lowPriorityPool().execute(tr);
      tr.blockTillStarted();
    } finally {
      for (BlockingTestRunnable btr : blockingRunnables) {
        btr.unblock();
      }
    }
  }
}
