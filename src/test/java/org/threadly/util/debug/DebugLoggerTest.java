package org.threadly.util.debug;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class DebugLoggerTest extends ThreadlyTester {
  @AfterEach
  public void cleanup() {
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
    String testStr = StringUtils.makeRandomString(5);
    DebugLogger.log(testStr);
    
    assertTrue(DebugLogger.getAllStoredMessages(false).equals(testStr));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getAllMultipleTest() {
    String testStr1 = StringUtils.makeRandomString(5);
    String testStr2 = StringUtils.makeRandomString(5);
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getAllStoredMessages(true);
    
    assertTrue(result.contains(testStr1));
    assertTrue(result.contains(testStr2));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtySingleTest() {
    String testStr = StringUtils.makeRandomString(5);
    DebugLogger.log(testStr);
    
    assertTrue(DebugLogger.getOldestLogMessages(2).equals(testStr));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtyMultipleTest() {
    String testStr1 = StringUtils.makeRandomString(5);
    String testStr2 = StringUtils.makeRandomString(5);
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getOldestLogMessages(2, true);
    
    assertTrue(result.contains(testStr1));
    assertTrue(result.contains(testStr2));
    
    assertEquals(0, DebugLogger.getAllStoredMessages().length());
  }
  
  @Test
  public void getQtyLimitedTest() {
    String testStr1 = StringUtils.makeRandomString(5);
    String testStr2 = StringUtils.makeRandomString(5);
    DebugLogger.log(testStr1);
    DebugLogger.log(testStr2);
    
    String result = DebugLogger.getOldestLogMessages(1, true);
    
    assertTrue(result.contains(testStr1));
    assertFalse(result.contains(testStr2));
    
    assertTrue(DebugLogger.getAllStoredMessages().length() > 0);
  }
}
