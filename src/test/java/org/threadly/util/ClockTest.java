package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.threadly.test.TestUtil;

public class ClockTest {
  @Before
  public void setup() {
    Clock.stopClockUpdateThread();
  }
  
  @Test
  public void testClock() {
    // verify clock is not updating
    long before = Clock.lastKnownTimeMillis();
    
    TestUtil.sleep(Clock.AUTOMATIC_UPDATE_FREQUENCY_IN_MS);
    
    assertEquals(before, Clock.lastKnownTimeMillis());
    
    // update clock
    long newTime;
    assertTrue((newTime = Clock.accurateTime()) > before);
    // verify we get the new time again
    assertEquals(newTime, Clock.lastKnownTimeMillis());
  }
  
  @Test
  public void verifyAutomaticUpdate() {
    long before = Clock.lastKnownTimeMillis();
    
    Clock.startClockUpdateThread();

    TestUtil.sleep(Clock.AUTOMATIC_UPDATE_FREQUENCY_IN_MS + 
                   (Clock.AUTOMATIC_UPDATE_FREQUENCY_IN_MS / 2));
    
    assertTrue(Clock.lastKnownTimeMillis() > before);
  }
}
