package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestUtils;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class ClockWrapperTest {
  @BeforeClass
  public static void setupClass() {
    Clock.stopClockUpdateThread();
  }
  
  @AfterClass
  public static void tearDownClass() {
    Clock.startClockUpdateThread();
  }
  
  @After
  public void tearDown() {
    ClockWrapper.REQUESTS_TO_STOP_UPDATING_TIME.set(0);
  }
  
  @Test
  public void getAccurateTimeTest() {
    final long startTime = ClockWrapper.getSemiAccurateTime();
    
    new TimeChangeCondition(startTime, true).blockTillTrue();
    
    // verify getting updates
    assertTrue(startTime != ClockWrapper.getSemiAccurateTime());
    
    // request stop to updates
    ClockWrapper.stopForcingUpdate();
    long updateTime = ClockWrapper.getSemiAccurateTime();

    new TimeChangeCondition(updateTime, false).blockTillTrue();
    
    // verify no longer getting updates
    assertEquals(updateTime, ClockWrapper.getSemiAccurateTime());
    
    // allow updates again
    ClockWrapper.resumeForcingUpdate();
    
    TestUtils.blockTillClockAdvances();
    
    assertTrue(updateTime != ClockWrapper.getSemiAccurateTime());
  }
  
  @Test (expected = IllegalStateException.class)
  public void resumeFail() {
    ClockWrapper.resumeForcingUpdate();
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
