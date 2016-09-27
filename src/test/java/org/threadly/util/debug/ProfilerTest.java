package org.threadly.util.debug;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.future.ListenableFuture;
import org.threadly.concurrent.statistics.PrioritySchedulerStatisticTracker;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int MIN_RESPONSE_LENGTH = 10;
  
  protected Profiler profiler;
  
  @Before
  public void setup() {
    profiler = new Profiler(POLL_INTERVAL);
  }
  
  @After
  public void cleanup() {
    profiler.stop();
    profiler = null;
  }
  
  protected void blockForProfilerSample() {
    new TestCondition() {
      @Override
      public boolean get() {
        return profiler.getCollectedSampleQty() > 0;
      }
    }.blockTillTrue(1000 * 20);
  }
  
  @Test
  public void constructorTest() {
    int testPollInterval = Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS * 10;
    Profiler p;
    
    p = new Profiler();
    assertNotNull(p.pStore.threadTraces);
    assertEquals(0, p.pStore.threadTraces.size());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pStore.pollIntervalInMs);
    assertNull(p.pStore.collectorThread.get());
    assertNull(p.pStore.dumpingThread);
    assertNotNull(p.startStopLock);
    
    p = new Profiler(testPollInterval);
    assertNotNull(p.pStore.threadTraces);
    assertEquals(0, p.pStore.threadTraces.size());
    assertEquals(testPollInterval, p.pStore.pollIntervalInMs);
    assertNull(p.pStore.collectorThread.get());
    assertNull(p.pStore.dumpingThread);
    assertNotNull(p.startStopLock);
  }
  
  @Test
  public void getProfileThreadsIteratorTest() {
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertNotNull(it.next());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void profileThreadsIteratorNextFail() {
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    
    while (it.hasNext()) {
      assertNotNull(it.next());
    }
    
    it.next();
    fail("Exception should have thrown");
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void profileThreadsIteratorRemoveFail() {
    Iterator<?> it = profiler.pStore.getProfileThreadsIterator();
    it.next();
    
    // not currently supported
    it.remove();
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new Profiler(-1);
  }
  
  @Test
  public void isRunningTest() {
    assertFalse(profiler.isRunning());
    
    /* verification of isRunning after start happens in 
     * startWithoutExecutorTest and startWitExecutorTest
     */
  }
  
  @Test
  public void startWithoutExecutorTest() {
    profiler.start(null);
    
    assertTrue(profiler.isRunning());
  }
  
  @Test
  public void startWitExecutorTest() {
    PrioritySchedulerStatisticTracker e = new PrioritySchedulerStatisticTracker(1);
    try {
      assertEquals(0, e.getActiveTaskCount());
      
      profiler.start(e);
      
      assertTrue(profiler.isRunning());
      assertEquals(1, e.getActiveTaskCount());
    } finally {
      profiler.stop();
      e.shutdownNow();
    }
  }
  
  @Test
  public void startWithTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    long start = Clock.accurateForwardProgressingMillis();
    ListenableFuture<String> lf = profiler.start(DELAY_TIME);
    String result = lf.get(DELAY_TIME + (10 * 1000), TimeUnit.MILLISECONDS);
    long end = Clock.accurateForwardProgressingMillis();

    // profiler should be stopped now
    assertFalse(profiler.isRunning());
    assertTrue(end - start >= DELAY_TIME);
    assertNotNull(result);
  }
  
  @Test
  public void startWitExecutorAndTimeoutTest() throws InterruptedException, ExecutionException, TimeoutException {
    StrictPriorityScheduler ps = new StrictPriorityScheduler(2);
    try {
      long start = Clock.accurateForwardProgressingMillis();
      ListenableFuture<String> lf = profiler.start(ps, DELAY_TIME);
      String result = lf.get(10 * 1000, TimeUnit.MILLISECONDS);
      long end = Clock.accurateForwardProgressingMillis();

      // profiler should be stopped now
      assertFalse(profiler.isRunning());
      assertTrue(end - start >= DELAY_TIME);
      assertNotNull(result);
    } finally {
      ps.shutdownNow();
    }
  }
  
  @Test
  public void getAndSetProfileIntervalTest() {
    int TEST_VAL = 100;
    profiler.setPollInterval(TEST_VAL);
    
    assertEquals(TEST_VAL, profiler.getPollInterval());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setProfileIntervalFail() {
    profiler.setPollInterval(-1);
  }
  
  @Test
  public void resetTest() {
    profiler.start();
    // verify there are some samples
    blockForProfilerSample();
    final Thread runningThread = profiler.pStore.collectorThread.get();
    profiler.stop();
    
    // verify stopped
    new TestCondition() {
      @Override
      public boolean get() {
        return ! runningThread.isAlive();
      }
    }.blockTillTrue(1000 * 20);
    
    profiler.reset();
    
    assertEquals(0, profiler.pStore.threadTraces.size());
    assertEquals(0, profiler.getCollectedSampleQty());
  }
  
  @Test
  public void dumpStoppedStringTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamTest() {
    profiler.start();
    
    blockForProfilerSample();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringOnlySummaryTest() {
    profiler.start();
    blockForProfilerSample();
    
    String resultStr = profiler.dump(false);
    
    assertTrue(resultStr.startsWith("Combined profile for all threads"));
  }
  
  protected static void verifyDumpStr(String resultStr) {
    assertTrue(resultStr.length() > MIN_RESPONSE_LENGTH);
    
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_COUNT_HEADER));
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_NET_HEADER));
  }
}
