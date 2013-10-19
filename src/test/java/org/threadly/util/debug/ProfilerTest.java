package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ProfilerTest {
  private static final int POLL_INTERVAL = 5;
  private static final int MIN_RESPONSE_LENGTH = 100;
  
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
  public void dumpStoppedStringTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    profiler.stop();
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpStringTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    String resultStr = profiler.dump();
    
    verifyDumpStr(resultStr);
  }
  
  @Test
  public void dumpOutputStreamTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    verifyDumpStr(resultStr);
  }
  
  private static void verifyDumpStr(String resultStr) {
    assertTrue(resultStr.length() > MIN_RESPONSE_LENGTH);
    
    assertTrue(resultStr.contains(Profiler.THREAD_DELIMITER));
    assertFalse(resultStr.startsWith(Profiler.THREAD_DELIMITER));
    assertFalse(resultStr.endsWith(Profiler.THREAD_DELIMITER));
    
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_COUNT_HEADER));
    assertTrue(resultStr.contains(Profiler.FUNCTION_BY_NET_HEADER));
    
    System.out.println(resultStr);
  }
}
