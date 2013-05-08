package org.threadly.test;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class TestUtilTest {
  @Test
  public void blockTillClockAdvancesTest() {
    long before = System.currentTimeMillis();
    TestUtil.blockTillClockAdvances();
    assertTrue(System.currentTimeMillis() != before);
  }
}
