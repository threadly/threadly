package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ClockWrapperTest {
  @Before
  public void setup() {
    Clock.stopClockUpdateThread();
  }
  
  @Test
  public void getAccurateTimeTest() {
    final long startTime = ClockWrapper.getAccurateTime();
    
    new TimeChangeCondition(startTime).blockTillTrue();
    
    long updateTime;
    // verify getting updates
    assertTrue(startTime != (updateTime = ClockWrapper.getAccurateTime()));
    
    // request stop to updates
    ClockWrapper.stopForcingUpdate();

    new TimeChangeCondition(updateTime).blockTillTrue();
    
    // verify no longer getting updates
    assertEquals(updateTime, ClockWrapper.getAccurateTime());
    
    // allow updates again
    ClockWrapper.resumeForcingUpdate();
    
    assertTrue(updateTime != ClockWrapper.getAccurateTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void resumeFail() {
    ClockWrapper.resumeForcingUpdate();
  }
  
  @Test
  public void getLastKnownTimeAndUpdateTest() {
    long originalTime;
    assertEquals(originalTime = Clock.lastKnownTimeMillis(), ClockWrapper.getLastKnownTime());

    new TimeChangeCondition(originalTime).blockTillTrue();
    
    long updateTime = ClockWrapper.updateClock();
    assertTrue(originalTime != updateTime);
    assertEquals(Clock.lastKnownTimeMillis(), ClockWrapper.getLastKnownTime());
    assertEquals(updateTime, ClockWrapper.getLastKnownTime());
  }
  
  private class TimeChangeCondition extends TestCondition {
    private final long time;
    
    private TimeChangeCondition(long time) {
      this.time = time;
    }
    
    @Override
    public boolean get() {
      return System.currentTimeMillis() != time;
    }
    
    @Override
    public void blockTillTrue() {
      blockTillTrue(100, 1);
    }
  }
}
