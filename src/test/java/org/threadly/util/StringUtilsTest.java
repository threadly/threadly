package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class StringUtilsTest {
  @Test
  public void makeNonNullTest() {
    assertNotNull(StringUtils.makeNonNull(null));
    assertTrue(StringUtils.makeNonNull(null).isEmpty());
    
    String testStr = "foo";
    assertTrue(StringUtils.makeNonNull(testStr) == testStr);
  }
  
  @Test
  public void randomStringTest() {
    String result = StringUtils.randomString(10);
    
    assertFalse(result.isEmpty());
    
    assertFalse(StringUtils.randomString(10).equals(result));
  }
  
  @Test
  public void randomStringEmptyTest() {
    String result = StringUtils.randomString(0);
    
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }
}
