package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class ProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int MIN_RESPONSE_LENGTH = 10;
  
  private Profiler profiler;
  
  @Before
  public void setUp() {
    profiler = new Profiler(POLL_INTERVAL);
  }
  
  @After
  public void tearDown() {
    profiler.stop();
    profiler = null;
  }
  
  private void blockForProfilerSample() {
    new TestCondition() {
      @Override
      public boolean get() {
        return profiler.getCollectedSampleQty() > 0;
      }
    }.blockTillTrue();
  }
  
  @Test
  public void constructorTest() {
    int testPollInterval = Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS * 10;
    File dumpFile = new File("foo");
    Profiler p;
    
    p = new Profiler();
    assertNotNull(p.threadTraces);
    assertEquals(0, p.threadTraces.size());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertNull(p.outputFile);
    assertNotNull(p.startStopLock);
    
    p = new Profiler(dumpFile);
    assertNotNull(p.threadTraces);
    assertEquals(0, p.threadTraces.size());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertEquals(dumpFile, p.outputFile);
    assertNotNull(p.startStopLock);
    
    p = new Profiler(testPollInterval);
    assertNotNull(p.threadTraces);
    assertEquals(0, p.threadTraces.size());
    assertEquals(testPollInterval, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertNull(p.outputFile);
    assertNotNull(p.startStopLock);
    
    p = new Profiler(dumpFile, testPollInterval);
    assertNotNull(p.threadTraces);
    assertEquals(0, p.threadTraces.size());
    assertEquals(testPollInterval, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertEquals(dumpFile, p.outputFile);
    assertNotNull(p.startStopLock);
  }
  
  @Test
  public void getProfileThreadsIteratorTest() {
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertNotNull(it.next());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void profileThreadsIteratorNextFail() {
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
    
    while (it.hasNext()) {
      assertNotNull(it.next());
    }
    
    it.next();
    fail("Exception should have thrown");
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void profileThreadsIteratorRemoveFail() {
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
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
  }
  
  @Test
  public void startWithoutExecutorTest() {
    profiler.start(null);
    
    assertTrue(profiler.isRunning());
  }
  
  @Test
  public void startWitExecutorTest() {
    PrioritySchedulerStatisticTracker e = new PrioritySchedulerStatisticTracker(1, 1, 1000);
    try {
      assertEquals(0, e.getCurrentPoolSize());
      assertEquals(0, e.getCurrentRunningCount());
      
      profiler.start(e);
      
      assertTrue(profiler.isRunning());
      assertEquals(1, e.getCurrentPoolSize());
      assertEquals(1, e.getCurrentRunningCount());
    } finally {
      profiler.stop();
      e.shutdownNow();
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
    final Thread runningThread = profiler.collectorThread.get();
    profiler.stop();
    
    // verify stopped
    new TestCondition() {
      @Override
      public boolean get() {
        return ! runningThread.isAlive();
      }
    }.blockTillTrue();
    
    profiler.reset();
    
    assertEquals(0, profiler.threadTraces.size());
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
  
  protected static void verifyDumpStr(String resultStr) {
    assertTrue(resultStr.length() > MIN_RESPONSE_LENGTH);
    
    assertFalse(resultStr.startsWith(Profiler.THREAD_DELIMITER));
    assertFalse(resultStr.endsWith(Profiler.THREAD_DELIMITER));
    
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_COUNT_HEADER));
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_NET_HEADER));
  }
}
