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
  public void systemNanoTimeTest() {
    long baseTime = System.nanoTime();
    assertTrue(Clock.systemNanoTime() >= baseTime);
  }
  
  @Test
  public void startClockUpdateThreadTwiceTest() {
    Clock.startClockUpdateThread();
    
    assertNotNull(Clock.clockUpdater);
    
    Runnable updater = Clock.clockUpdater;
    
    Clock.startClockUpdateThread();
    
    // should point to the same reference
    assertTrue(updater == Clock.clockUpdater);
  }
  
  @Test
  public void timeSinceClockStartMillisTest() {
    long timeSinceClockStartMillis = Clock.timeSinceClockStartMillis();
    assertTrue(timeSinceClockStartMillis >= 0);
    assertTrue(timeSinceClockStartMillis < 1000 * 60 * 15); // less than 15 min
  }
  
  @Test
  public void accurateTimeSinceClockStartMillisTest() {
    final long timeSinceClockStartMillis = Clock.accurateTimeSinceClockStartMillis();
    assertTrue(timeSinceClockStartMillis >= 0);
    assertTrue(timeSinceClockStartMillis < 1000 * 60 * 15); // less than 15 min
    
    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.accurateTimeSinceClockStartMillis() > timeSinceClockStartMillis;
      }
    }.blockTillTrue(200);
  }
  
  @Test
  public void alwaysProgressingLastKnownTimeMillisTest() {
    // verify clock is not updating
    long before = Clock.alwaysProgressingLastKnownTimeMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // update clock
    long newTime = -1;
    assertTrue((newTime = Clock.alwaysProgressingAccurateTimeMillis()) > before);
    // verify we get the new time again
    assertTrue(newTime <= Clock.alwaysProgressingLastKnownTimeMillis());
  }
  
  @Test
  public void automaticAlwaysProgressingUpdateTest() {
    Clock.startClockUpdateThread();
    final long before = Clock.alwaysProgressingLastKnownTimeMillis();

    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.alwaysProgressingLastKnownTimeMillis() > before;
      }
    }.blockTillTrue(500);
  }
  
  @Test
  public void accurateTimeMillisTest() {
    final long startTime = Clock.accurateTimeMillis();
    
    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.accurateTimeMillis() > startTime;
      }
    }.blockTillTrue(200);
  }
  
  @Test
  public void lastKnownTimeMillisTest() {
    // verify clock is not updating
    long before = Clock.lastKnownTimeMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // update clock
    long newTime = -1;
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
    }.blockTillTrue(500);
  }
}
