package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.PrioritySchedulerStatisticTracker;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ControlledThreadProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int WAIT_TIME_FOR_COLLECTION = 50;
  private static final int MIN_RESPONSE_LENGTH = 10;
  
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
  
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ControlledThreadProfiler(-1);
  }
  
  @Test
  public void addProfiledThreadTest() {
    assertEquals(0, profiler.profiledThreads.size());
    
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
  public void removeProfiledThreadTest() {
    assertEquals(0, profiler.profiledThreads.size());
    
    Thread thread1 = new Thread();
    Thread thread2 = new Thread();
    profiler.addProfiledThread(thread1);
    profiler.addProfiledThread(thread2);
    
    assertEquals(2, profiler.profiledThreads.size());
    
    assertTrue(profiler.removedProfiledThread(thread1));
    
    assertEquals(1, profiler.profiledThreads.size());
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
      assertEquals(0, e.getCurrentlyRunningCount());
      
      profiler.start(e);
      
      assertTrue(profiler.isRunning());
      assertEquals(1, e.getCurrentPoolSize());
      assertEquals(1, e.getCurrentlyRunningCount());
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
  public void dumpStoppedStringEmptyTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpStoppedStringTest() throws IOException {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamEmptyTest() throws IOException {
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
  public void dumpStoppedOutputStreamTest() throws IOException {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringEmptyTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    String resultStr = profiler.dump();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpStringTest() throws IOException {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamEmptyTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    // no dump since no threads set to track
    assertEquals(0, resultStr.length());
  }
  
  @Test
  public void dumpOutputStreamTest() throws IOException {
    profiler.addProfiledThread(Thread.currentThread());
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  private static void verifyDumpStr(String resultStr) {
    assertTrue(resultStr.length() > MIN_RESPONSE_LENGTH);
    
    assertFalse(resultStr.startsWith(Profiler.THREAD_DELIMITER));
    assertFalse(resultStr.endsWith(Profiler.THREAD_DELIMITER));
    
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_COUNT_HEADER));
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_NET_HEADER));
  }
}
