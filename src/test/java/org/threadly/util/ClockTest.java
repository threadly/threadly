package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ClockTest extends ThreadlyTester {
  @AfterAll
  public static void cleanupClass() {
    Clock.startClockUpdateThread();
  }
  
  @BeforeEach
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
    
    new TestCondition(() -> Clock.accurateForwardProgressingMillis() > timeSinceClockStartMillis)
        .blockTillTrue(200);
  }
  
  @Test
  public void forwardProgressingDurationTest() {
    long startTime = Clock.lastKnownForwardProgressingMillis();
    
    assertEquals(0, Clock.forwardProgressingDuration(startTime));
    
    TestUtils.blockTillClockAdvances();
    
    assertTrue(Clock.forwardProgressingDuration(startTime) > 0);
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

    new TestCondition(() -> Clock.lastKnownForwardProgressingMillis() > before).blockTillTrue(1000);
  }
  
  @Test
  public void accurateTimeMillisTest() {
    final long startTime = Clock.accurateTimeMillis();
    
    new TestCondition(() -> Clock.accurateTimeMillis() > startTime).blockTillTrue(200);
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

    new TestCondition(() -> Clock.lastKnownTimeMillis() > before).blockTillTrue(1000);
  }
}
