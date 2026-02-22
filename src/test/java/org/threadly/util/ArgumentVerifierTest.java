package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class ArgumentVerifierTest extends ThreadlyTester {
  @Test
  public void assertNotNullTest() {
    ArgumentVerifier.assertNotNull(new Object(), "foo");
    // should not throw
  }
  
  @Test
  public void assertNotNullFail() {
    String name = StringUtils.makeRandomString(5);
    try {
      ArgumentVerifier.assertNotNull(null, name);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(name));
    }
  }
  
  @Test
  public void assertNotNegativeTest() {
    ArgumentVerifier.assertNotNegative(0, "foo");
    // should not throw
  }
  
  @Test
  public void assertNotNegativeFail() {
    String name = StringUtils.makeRandomString(5);
    try {
      ArgumentVerifier.assertNotNegative(-1, name);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(name));
    }
  }
  
  @Test
  public void assertGreaterThanZeroTest() {
    ArgumentVerifier.assertGreaterThanZero(1, "foo");
    // should not throw
  }
  
  @Test
  public void assertGreaterThanZeroFail() {
    String name = StringUtils.makeRandomString(5);
    try {
      ArgumentVerifier.assertGreaterThanZero(0, name);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(name));
    }
  }
  
  @Test
  public void assertLessThanTest() {
    ArgumentVerifier.assertLessThan(10, 20, "foo");
    // should not throw
  }
  
  @Test
  public void assertLessThanFail() {
    String name = StringUtils.makeRandomString(5);
    try {
      ArgumentVerifier.assertLessThan(10, 10, name);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(name));
    }
  }
}
