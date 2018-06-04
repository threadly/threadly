package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayDeque;
import java.util.Queue;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class TestConditionTest extends ThreadlyTester {
  @Test
  public void blockTest() {
    DelayCondition dc = new DelayCondition(DELAY_TIME);
    long startTime = Clock.accurateForwardProgressingMillis();
    dc.blockTillTrue((DELAY_TIME * 10) + 5000);
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
    assertTrue(endTime - startTime <= DELAY_TIME + (SLOW_MACHINE ? 5000 : 1000));
    
    startTime = Clock.accurateForwardProgressingMillis();
    dc.blockTillTrue(); // should return immediately
    assertTrue(Clock.accurateForwardProgressingMillis() - startTime <= 10);
  }
  
  @Test
  public void blockFail() {
    DelayCondition dc = new DelayCondition(DELAY_TIME * 100);
    long startTime = Clock.accurateForwardProgressingMillis();
    try {
      dc.blockTillTrue(DELAY_TIME, 1);
      fail("Exception should have been thrown");
    } catch (TestCondition.ConditionTimeoutException e) {
      assertTrue(Clock.accurateForwardProgressingMillis() - startTime >= DELAY_TIME);
    }
  }
  
  @Test (expected = RuntimeException.class)
  public void defaultConstructorFail() {
    TestCondition tc = new TestCondition();
    tc.get();
  }
  
  @Test
  public void supplierPredicateTest() {
    Queue<String> values = new ArrayDeque<>(8);
    values.add("foo");
    values.add("false");
    values.add("true");
    
    new TestCondition(() -> values.remove(), Boolean::parseBoolean).blockTillTrue();
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
        firstGetTime = Clock.accurateForwardProgressingMillis();
        return false;
      }
      
      return Clock.accurateForwardProgressingMillis() - firstGetTime >= delayTime;
    }
  }
}
