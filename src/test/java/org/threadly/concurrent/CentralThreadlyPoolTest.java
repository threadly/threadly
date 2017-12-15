package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.threadly.BlockingTestRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class CentralThreadlyPoolTest {
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
        CentralThreadlyPool.lowPriorityPool(false).execute(tr);
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
  public void threadPoolsGuaranteedThreadTest() {
    int threadsPerScheduler = 10;
    List<SchedulerService> executors = new ArrayList<>();
    for (int i = 0; i < TEST_QTY * 2; i++) {
      executors.add(CentralThreadlyPool.threadPool(threadsPerScheduler));
    }
    verifyGuaranteedThreadProtection(executors, threadsPerScheduler);
  }
}
