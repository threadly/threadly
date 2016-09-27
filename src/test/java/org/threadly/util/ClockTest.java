package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ClockTest {
  @AfterClass
  public static void cleanupClass() {
    Clock.startClockUpdateThread();
  }
  
  @Before
  public void setup() {
    Clock.stopClockUpdateThread();
  }
  
  @Test
  public void accurateTimeNanosTest() {
    long baseTime = System.nanoTime();
    assertTrue(Clock.accurateTimeNanos() >= baseTime);
  }
  
  @Test
  public void lastKnownTimeNanosTest() {
    // verify clock is not updating
    long before = Clock.lastKnownTimeNanos();
    
    TestUtils.blockTillClockAdvances();
    
    // update clock
    long newTime = -1;
    assertTrue((newTime = Clock.accurateTimeNanos()) > before);
    // verify we get the new time again
    assertEquals(newTime, Clock.lastKnownTimeNanos());
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
  public void lastKnownForwardProgressingMillisTest() {
    long timeSinceClockStartMillis = Clock.lastKnownForwardProgressingMillis();
    assertTrue(timeSinceClockStartMillis >= 0);
    assertTrue(timeSinceClockStartMillis < 1000 * 60 * 15); // less than 15 min
  }
  
  @Test
  public void accurateForwardProgressingMillisTest() {
    final long timeSinceClockStartMillis = Clock.accurateForwardProgressingMillis();
    assertTrue(timeSinceClockStartMillis >= 0);
    assertTrue(timeSinceClockStartMillis < 1000 * 60 * 15); // less than 15 min
    
    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.accurateForwardProgressingMillis() > timeSinceClockStartMillis;
      }
    }.blockTillTrue(200);
  }
  
  @Test
  public void lastKnownForwardProgressingMillisAccurateTimeUpdateTest() {
    // verify clock is not updating
    long before = Clock.lastKnownForwardProgressingMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // update clock
    long newTime = -1;
    assertTrue((newTime = Clock.accurateForwardProgressingMillis()) > before);
    // verify we get the new time again
    assertTrue(newTime == Clock.lastKnownForwardProgressingMillis());
  }
  
  @Test
  public void automaticAlwaysProgressingUpdateTest() {
    Clock.startClockUpdateThread();
    final long before = Clock.lastKnownForwardProgressingMillis();

    new TestCondition() {
      @Override
      public boolean get() {
        return Clock.lastKnownForwardProgressingMillis() > before;
      }
    }.blockTillTrue(1000);
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
    assertEquals(newTime, Clock.lastKnownTimeMillis());
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
    }.blockTillTrue(1000);
  }
}
