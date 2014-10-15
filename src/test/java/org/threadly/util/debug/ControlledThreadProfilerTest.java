package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ControlledThreadProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int WAIT_TIME_FOR_COLLECTION = 50;
  
  private ControlledThreadProfiler profiler;
  
  @Before
  public void setUp() {
    profiler = new ControlledThreadProfiler(POLL_INTERVAL);
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
    ControlledThreadProfiler p;
    
    p = new ControlledThreadProfiler();
    assertNotNull(p.threadTraces);
    assertTrue(p.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertNull(p.outputFile);
    assertNotNull(p.startStopLock);
    assertTrue(p.profiledThreads.isEmpty());
    
    p = new ControlledThreadProfiler(dumpFile);
    assertNotNull(p.threadTraces);
    assertTrue(p.threadTraces.isEmpty());
    assertEquals(Profiler.DEFAULT_POLL_INTERVAL_IN_MILLIS, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertEquals(dumpFile, p.outputFile);
    assertNotNull(p.startStopLock);
    assertTrue(p.profiledThreads.isEmpty());
    
    p = new ControlledThreadProfiler(testPollInterval);
    assertNotNull(p.threadTraces);
    assertTrue(p.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertNull(p.outputFile);
    assertNotNull(p.startStopLock);
    assertTrue(p.profiledThreads.isEmpty());
    
    p = new ControlledThreadProfiler(dumpFile, testPollInterval);
    assertNotNull(p.threadTraces);
    assertTrue(p.threadTraces.isEmpty());
    assertEquals(testPollInterval, p.pollIntervalInMs);
    assertNull(p.collectorThread.get());
    assertNull(p.dumpingThread);
    assertEquals(dumpFile, p.outputFile);
    assertNotNull(p.startStopLock);
    assertTrue(p.profiledThreads.isEmpty());
  }
  
  @Test
  public void getProfileThreadsIteratorEmptyTest() {
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertFalse(it.hasNext());
  }
  
  @Test
  public void getProfileThreadsIteratorTest() {
    profiler.addProfiledThread(Thread.currentThread());
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertTrue(it.next() == Thread.currentThread());
    // should only have the one added thread
    assertFalse(it.hasNext());
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ControlledThreadProfiler(-1);
  }
  
  @Test
  public void addProfiledThreadTest() {
    assertTrue(profiler.profiledThreads.isEmpty());
    
    Thread currentThread = Thread.currentThread();
    profiler.addProfiledThread(currentThread);
    
    assertEquals(1, profiler.profiledThreads.size());
  }
  
  @Test
  public void addProfiledThreadDuplicateThreadTest() {
    assertEquals(0, profiler.profiledThreads.size());
    
    Thread currentThread = Thread.currentThread();
    profiler.addProfiledThread(currentThread);
    
    assertEquals(1, profiler.profiledThreads.size());
    
    profiler.addProfiledThread(currentThread);
    // nothing should have changed
    assertEquals(1, profiler.profiledThreads.size());
  }
  
  @Test
  public void addNullProfiledThreadTest() {
    assertTrue(profiler.profiledThreads.isEmpty());
    
    profiler.addProfiledThread(null);
    
    assertTrue(profiler.profiledThreads.isEmpty());
  }
  
  @Test
  public void removeProfiledThreadTest() {
    assertTrue(profiler.profiledThreads.isEmpty());
    
    Thread thread1 = new Thread();
    Thread thread2 = new Thread();
    profiler.addProfiledThread(thread1);
    profiler.addProfiledThread(thread2);
    
    assertEquals(2, profiler.profiledThreads.size());
    
    assertTrue(profiler.removedProfiledThread(thread1));
    
    assertEquals(1, profiler.profiledThreads.size());
  }
  
  @Test
  public void getProfiledThreadCountTest() {
    int testThreadCount = 10;
    assertEquals(0, profiler.getProfiledThreadCount());
    
    List<Thread> addedThreads = new ArrayList<Thread>(testThreadCount);
    for (int i = 0; i < testThreadCount; i++) {
      Thread t = new Thread();
      addedThreads.add(t);
      profiler.addProfiledThread(t);
      assertEquals(i + 1, profiler.getProfiledThreadCount());
    }
    
    Iterator<Thread> it = addedThreads.iterator();
    int removedCount = 0;
    while (it.hasNext()) {
      Thread t = it.next();
      profiler.removedProfiledThread(t);
      removedCount++;
      assertEquals(testThreadCount - removedCount, profiler.getProfiledThreadCount());
    }
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
  public void dumpStoppedStringEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpStoppedStringTest() {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    ProfilerTest.verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    blockForProfilerSample();
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    ProfilerTest.verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpStringTest() {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    blockForProfilerSample();
    
    String resultStr = profiler.dump();
    
    ProfilerTest.verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamEmptyTest() {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpOutputStreamTest() {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    blockForProfilerSample();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    ProfilerTest.verifyDumpStr(resultStr);
  }
}
