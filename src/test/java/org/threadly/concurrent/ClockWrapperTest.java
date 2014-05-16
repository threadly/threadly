package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ClockWrapperTest {
  private ClockWrapper clockWrapper;
  
  @Before
  public void setup() {
    clockWrapper = new ClockWrapper();
  }
  
  @After
  public void tearDown() {
    clockWrapper = null;
  }
  
  @Test
  public void getAccurateTimeTest() {
    final long startTime = clockWrapper.getSemiAccurateTime();
    
    new TimeChangeCondition(startTime, true).blockTillTrue();
    
    // verify getting updates
    assertTrue(startTime != clockWrapper.getSemiAccurateTime());
    
    // request stop to updates
    clockWrapper.stopForcingUpdate();
    long updateTime = clockWrapper.getSemiAccurateTime();

    new TimeChangeCondition(updateTime, false).blockTillTrue();
    
    // verify no longer getting updates
    assertEquals(updateTime, clockWrapper.getSemiAccurateTime());
    
    // allow updates again
    clockWrapper.resumeForcingUpdate();
    
    TestUtils.blockTillClockAdvances();
    
    assertTrue(updateTime != clockWrapper.getSemiAccurateTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void resumeFail() {
    clockWrapper.resumeForcingUpdate();
  }
  
  private class TimeChangeCondition extends TestCondition {
    private final boolean system;
    private final long time;
    
    private TimeChangeCondition(long time, boolean system) {
      this.system = system;
      this.time = time;
    }
    
    @Override
    public boolean get() {
      if (system) {
        return System.currentTimeMillis() != time;
      } else {
        return Clock.accurateTime() != time;
      }
    }
    
    @Override
    public void blockTillTrue() {
      blockTillTrue(100, 1);
    }
  }
}
