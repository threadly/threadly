package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ClockTest {
  @BeforeClass
  public static void setupClass() {
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
    long before = Clock.lastKnownTimeMillis();
    
    Clock.startClockUpdateThread();

    TestUtils.sleep(Clock.AUTOMATIC_UPDATE_FREQUENCY_IN_MS + 
                   (Clock.AUTOMATIC_UPDATE_FREQUENCY_IN_MS / 2));
    
    assertTrue(Clock.lastKnownTimeMillis() > before);
  }
}
