package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestUtils;

@SuppressWarnings("javadoc")
public class ClockWrapperTest {
  @Test
  public void getAccurateTimeTest() {
    final long startTime = ClockWrapper.getSemiAccurateMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // verify getting updates
    assertTrue(startTime != ClockWrapper.getSemiAccurateMillis());
  }
  
  @Test
  public void getAccurateTimeAfterStoppingUpdatesTest() {
    // request stop to updates
    ClockWrapper.stopForcingUpdate();
    long updateTime = ClockWrapper.getSemiAccurateMillis();
    
    TestUtils.blockTillClockAdvances();
    
    // verify no longer getting updates
    assertEquals(updateTime, ClockWrapper.getSemiAccurateMillis());
    
    // allow updates again
    ClockWrapper.resumeForcingUpdate();
    
    TestUtils.blockTillClockAdvances();
    
    assertTrue(updateTime != ClockWrapper.getSemiAccurateMillis());
  }
}
