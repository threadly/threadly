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

    assertTrue(resultStr.length() > 1000);
  }
  
  @Test
  public void dumpStoppedOutputStreamTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    profiler.stop();
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    assertTrue(resultStr.length() > 1000);
  }
  
  @Test
  public void dumpStringTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    String resultStr = profiler.dump();

    assertTrue(resultStr.length() > 1000);
  }
  
  @Test
  public void dumpOutputStreamTest() throws IOException {
    profiler.start();
    
    TestUtils.sleep(POLL_INTERVAL * 10);
    
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    profiler.dump(out);
    
    String resultStr = out.toString();
    
    assertTrue(resultStr.length() > 1000);
  }
}
