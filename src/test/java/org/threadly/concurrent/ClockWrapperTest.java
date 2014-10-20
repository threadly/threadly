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
    
    new TimeChangeCondition(startTime).blockTillTrue();
    
    // verify getting updates
    assertTrue(startTime != clockWrapper.getSemiAccurateTime());
    
    // request stop to updates
    clockWrapper.stopForcingUpdate();
    long updateTime = clockWrapper.getSemiAccurateTime();

    new TimeChangeCondition(updateTime).blockTillTrue();
    
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
    private final long time;
    
    private TimeChangeCondition(long time) {
      this.time = time;
    }
    
    @Override
    public boolean get() {
      return Clock.alwaysProgressingAccurateTimeMillis() != time;
    }
    
    @Override
    public void blockTillTrue() {
      blockTillTrue(200, 1);
    }
  }
}
