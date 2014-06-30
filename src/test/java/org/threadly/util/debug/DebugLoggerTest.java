package org.threadly.util.debug;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class DebugLoggerTest {
  @After
  public void tearDown() {
    DebugLogger.getAllStoredMessages(); // should clear state
  }
  
  @Test
  public void getCurrentMessageQtyTest() {
    // make sure debug logger is clear
    DebugLogger.getAllStoredMessages(false);
    
    assertEquals(0, DebugLogger.getCurrentMessageQty());
    
    DebugLogger.log("testMsg");
    
    assertEquals(1, DebugLogger.getCurrentMessageQty());
  }
  
  @Test
  public void getAllSingleTest() {
    String testStr = "foo";
    DebugLogger.log(testStr);
    
    assertTrue(DebugLogger.getAllStoredMessages(false).equals(testStr));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getAllMultipleTest() {
    String testStr1 = "foo";
    String testStr2 = "bar";
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getAllStoredMessages(true);
    
    assertTrue(result.contains(testStr1));
    assertTrue(result.contains(testStr2));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtySingleTest() {
    String testStr = "foo";
    DebugLogger.log(testStr);
    
    assertTrue(DebugLogger.getOldestLogMessages(2).equals(testStr));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtyMultipleTest() {
    String testStr1 = "foo";
    String testStr2 = "bar";
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getOldestLogMessages(2, true);
    
    assertTrue(result.contains(testStr1));
    assertTrue(result.contains(testStr2));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtyLimitedTest() {
    String testStr1 = "foo";
    String testStr2 = "bar";
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getOldestLogMessages(1, true);
    
    assertTrue(result.contains(testStr1));
    assertFalse(result.contains(testStr2));
    
    assertTrue(DebugLogger.getAllStoredMessages().length() > 0);
  }
}
