package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.threadly.test.TestUtil;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ClockWrapperTest {
  @Before
  public void setup() {
    Clock.stopClockUpdateThread();
  }
  
  @Test
  public void getAccurateTimeTest() {
    long startTime = ClockWrapper.getAccurateTime();
    
    TestUtil.sleep(10);
    
    long updateTime;
    // verify getting updates
    assertTrue(startTime != (updateTime = ClockWrapper.getAccurateTime()));
    
    // request stop to updates
    ClockWrapper.stopForcingUpdate();

    TestUtil.sleep(10);
    
    // verify no longer getting updates
    assertEquals(updateTime, ClockWrapper.getAccurateTime());
    
    // allow updates again
    ClockWrapper.resumeForcingUpdate();
    
    assertTrue(updateTime != ClockWrapper.getAccurateTime());
  }
  
  @Test
  public void getLastKnownTimeAndUpdateTest() {
    long originalTime;
    assertEquals(originalTime = Clock.lastKnownTimeMillis(), ClockWrapper.getLastKnownTime());

    TestUtil.sleep(10);
    
    long updateTime = ClockWrapper.updateClock();
    assertTrue(originalTime != updateTime);
    assertEquals(Clock.lastKnownTimeMillis(), ClockWrapper.getLastKnownTime());
    assertEquals(updateTime, ClockWrapper.getLastKnownTime());
  }
}
