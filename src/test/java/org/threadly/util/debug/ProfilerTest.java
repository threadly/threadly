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
public class ProfilerTest {
  private static final int POLL_INTERVAL = 1;
  private static final int WAIT_TIME_FOR_COLLECTION = 50;
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
  
  @Test
  public void getProfileThreadsIteratorTest() {
    Iterator<Thread> it = profiler.getProfileThreadsIterator();
    
    assertNotNull(it);
    assertTrue(it.hasNext());
    assertNotNull(it.next());
  }
  
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
  public void dumpStoppedStringTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(WAIT_TIME_FOR_COLLECTION);
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamTest() throws IOException {
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
