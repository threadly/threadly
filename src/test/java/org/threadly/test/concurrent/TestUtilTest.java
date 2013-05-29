package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class TestUtilTest {
  @Test
  public void blockTillClockAdvancesTest() {
    long before = System.currentTimeMillis();
    TestUtils.blockTillClockAdvances();
    assertTrue(System.currentTimeMillis() != before);
  }
}
