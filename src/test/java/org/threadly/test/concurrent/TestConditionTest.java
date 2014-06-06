package org.threadly.test.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestCondition;

@SuppressWarnings("javadoc")
public class TestConditionTest {
  @Test
  public void blockTest() {
    DelayCondition dc = new DelayCondition(DELAY_TIME);
    long startTime = System.currentTimeMillis();
    dc.blockTillTrue((DELAY_TIME * 10) + 500);
    long endTime = System.currentTimeMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
    assertTrue(endTime - startTime <= DELAY_TIME + 1000);
    
    startTime = System.currentTimeMillis();
    dc.blockTillTrue(); // should return immediately
    assertTrue(System.currentTimeMillis() - startTime <= 10);
  }
  
  @Test
  public void blockFail() {
    DelayCondition dc = new DelayCondition(DELAY_TIME * 100);
    long startTime = System.currentTimeMillis();
    try {
      dc.blockTillTrue(DELAY_TIME, 1);
      fail("Exception should have been thrown");
    } catch (TestCondition.ConditionTimeoutException e) {
      assertTrue(System.currentTimeMillis() - startTime >= DELAY_TIME);
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
