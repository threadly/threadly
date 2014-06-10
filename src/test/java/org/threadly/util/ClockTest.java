package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ClockTest {
  @Before
  public void setup() {
    Clock.stopClockUpdateThread();
  }
  
  @AfterClass
  public static void tearDownClass() {
    Clock.startClockUpdateThread();
  }
  
  @Test
  public void lastKnownTimeMillisTest() {
    // verify clock is not updating
    long before = Clock.lastKnownTimeMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // update clock
    long newTime;
    assertTrue((newTime = Clock.accurateTimeMillis()) > before);
    // verify we get the new time again
    assertTrue(newTime <= Clock.lastKnownTimeMillis());
  }
  
  @Test
  public void automaticUpdateTest() {
    final long before = Clock.lastKnownTimeMillis();
    
    Clock.startClockUpdateThread();

    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.lastKnownTimeMillis() > before;
      }
    }.blockTillTrue();
  }
}
