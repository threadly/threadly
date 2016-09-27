package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class StringUtilsTest {
  @Test
  public void nullToEmptyTest() {
    assertNotNull(StringUtils.nullToEmpty(null));
    assertTrue(StringUtils.nullToEmpty(null).isEmpty());
    
    String testStr = "foo";
    assertTrue(StringUtils.nullToEmpty(testStr) == testStr);
  }
  
  @Test
  public void emptyToNullTest() {
    assertNull(StringUtils.emptyToNull(null));
    assertNull(StringUtils.emptyToNull(""));
    
    String testStr = "foo";
    assertTrue(StringUtils.emptyToNull(testStr) == testStr);
  }
  
  @Test
  public void isNullOrEmptyTest() {
    assertTrue(StringUtils.isNullOrEmpty(null));
    assertTrue(StringUtils.isNullOrEmpty(""));
    assertFalse(StringUtils.isNullOrEmpty("1"));
  }
  
  @Test
  public void padStartNullTest() {
    assertEquals("00", StringUtils.padStart(null, 2, '0'));
  }
  
  @Test
  public void padStartExceededLengthTest() {
    String startStr = StringUtils.makeRandomString(10);
    assertEquals(startStr, StringUtils.padStart(startStr, 2, '0'));
  }
  
  @Test
  public void padStartTest() {
    String startStr = StringUtils.makeRandomString(5);
    String resultStr = StringUtils.padStart(startStr, 10, '0');
    assertEquals(10, resultStr.length());
    assertEquals(StringUtils.padStart(null, 5, '0'), resultStr.substring(0, 5));
    assertEquals(startStr, resultStr.substring(5));
  }
  
  @Test
  public void padEndNullTest() {
    assertEquals("00", StringUtils.padEnd(null, 2, '0'));
  }
  
  @Test
  public void padEndExceededLengthTest() {
    String startStr = StringUtils.makeRandomString(10);
    assertEquals(startStr, StringUtils.padEnd(startStr, 2, '0'));
  }
  
  @Test
  public void padEndTest() {
    String startStr = StringUtils.makeRandomString(5);
    String resultStr = StringUtils.padEnd(startStr, 10, '0');
    assertEquals(10, resultStr.length());
    assertEquals(startStr, resultStr.substring(0, 5));
    assertEquals(StringUtils.padStart(null, 5, '0'), resultStr.substring(5));
  }
  
  @Test
  public void makeRandomStringTest() {
    String result = StringUtils.makeRandomString(10);
    
    assertFalse(result.isEmpty());
    
    assertFalse(StringUtils.makeRandomString(10).equals(result));
  }
  
  @Test
  public void makeRandomStringEmptyTest() {
    String result = StringUtils.makeRandomString(0);
    
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }
}
