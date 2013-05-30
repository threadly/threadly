package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class TestConditionTest {
  private static final long DELAY_TIME = 100;
  
  @Test
  public void testBlock() {
    int pollInterval = 20;
    
    DelayCondition dc = new DelayCondition(DELAY_TIME);
    long startTime = System.currentTimeMillis();
    dc.blockTillTrue(1000, pollInterval);
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
    assertTrue(endTime - startTime <= DELAY_TIME + (pollInterval * 5));
    
    startTime = System.currentTimeMillis();
    dc.blockTillTrue(); // should return immediately
    assertTrue(System.currentTimeMillis() - startTime <= 10);
  }
  
  @Test
  public void testBlockFail() {
    int timeout = 10;
    
    DelayCondition dc = new DelayCondition(1000);
    long startTime = System.currentTimeMillis();
    try {
      dc.blockTillTrue(timeout, 1);
      fail("Exception should have been thrown");
    } catch (TestCondition.TimeoutException e) {
      assertTrue(System.currentTimeMillis() - startTime >= timeout);
    }
  }
  
  private class DelayCondition extends TestCondition {
    private final long delayTime;
    private long firstGetTime;
    
    private DelayCondition(long delayTime) {
      this.delayTime = delayTime;
      firstGetTime = -1;
    }
    
    @Override
    public boolean get() {
      if (firstGetTime < 0) {
        firstGetTime = System.currentTimeMillis();
        return false;
      }
      
      return System.currentTimeMillis() - firstGetTime >= delayTime;
    }
    
  }
}
