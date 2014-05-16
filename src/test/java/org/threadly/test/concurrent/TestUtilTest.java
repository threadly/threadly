package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class TestUtilTest {
  @Test
  public void blockTillClockAdvancesTest() {
    long before = Clock.accurateTimeMillis();
    TestUtils.blockTillClockAdvances();
    assertTrue(Clock.lastKnownTimeMillis() != before);
  }
}
