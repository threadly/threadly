package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.threadly.util.Clock;

@SuppressWarnings("javadoc")
public class SchedulingUtilsTest {
  private static long getNowToHour() {
    long hours = Clock.lastKnownTimeMillis() / TimeUnit.HOURS.toMillis(1);
    return TimeUnit.HOURS.toMillis(hours);
  }
  
  private static long getNowToDay() {
    long days = Clock.lastKnownTimeMillis() / TimeUnit.DAYS.toMillis(1);
    return TimeUnit.DAYS.toMillis(days);
  }
  
  @Test
  public void getDelayTillMinuteBeforeMinuteTest() {
    final int currentMinute = 10;
    final int toMinute = 30;
    
    long now = getNowToHour();
    now += TimeUnit.MINUTES.toMillis(currentMinute);
    
    long result = SchedulingUtils.getDelayTillMinute(now, toMinute);
    
    assertEquals(TimeUnit.MINUTES.toMillis(toMinute - currentMinute), result);
  }
  
  @Test
  public void getDelayTillMinuteAfterMinuteTest() {
    final int currentMinute = 50;
    final int toMinute = 10;
    
    long now = getNowToHour();
    now += TimeUnit.MINUTES.toMillis(currentMinute);
    
    long result = SchedulingUtils.getDelayTillMinute(now, toMinute);
    
    assertEquals(TimeUnit.MINUTES.toMillis(60 - currentMinute + toMinute), result);
  }
  
  @Test
  public void getDelayTillMinuteFail() {
    try {
      SchedulingUtils.getDelayTillMinute(-1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      SchedulingUtils.getDelayTillMinute(60);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getDelayTillHourBeforeHourTest() {
    final int minute = 10;
    final int currentHour = 16;
    final int toHour = 20;
    
    long now = getNowToDay();
    now += TimeUnit.HOURS.toMillis(currentHour);
    now += TimeUnit.MINUTES.toMillis(minute);
    
    long result = SchedulingUtils.getDelayTillHour(now, toHour, minute);
    
    assertEquals(TimeUnit.HOURS.toMillis(toHour - currentHour), result);
  }
  
  @Test
  public void getDelayTillHourAfterHourTest() {
    final int minute = 10;
    final int currentHour = 20;
    final int toHour = 2;
    
    long now = getNowToDay();
    now += TimeUnit.HOURS.toMillis(currentHour);
    now += TimeUnit.MINUTES.toMillis(minute);
    
    long result = SchedulingUtils.getDelayTillHour(now, toHour, minute);
    
    assertEquals(TimeUnit.HOURS.toMillis(24 - currentHour + toHour), result);
  }
  
  @Test
  public void getDelayTillHourBeforeMinuteTest() {
    final int currentMinute = 10;
    final int toMinute = 30;
    final int currentHour = 19;
    final int toHour = 20;
    
    long now = getNowToDay();
    now += TimeUnit.HOURS.toMillis(currentHour);
    now += TimeUnit.MINUTES.toMillis(currentMinute);
    
    long result = SchedulingUtils.getDelayTillHour(now, toHour, toMinute);
    
    assertEquals(TimeUnit.HOURS.toMillis(toHour - currentHour) + 
                   TimeUnit.MINUTES.toMillis(toMinute) - 
                   TimeUnit.MINUTES.toMillis(currentMinute), result);
  }
  
  @Test
  public void getDelayTillHourAfterMinuteTest() {
    final int currentMinute = 50;
    final int toMinute = 10;
    final int currentHour = 19;
    final int toHour = 20;
    
    long now = getNowToDay();
    now += TimeUnit.HOURS.toMillis(currentHour);
    now += TimeUnit.MINUTES.toMillis(currentMinute);
    
    long result = SchedulingUtils.getDelayTillHour(now, toHour, toMinute);
    
    assertEquals(TimeUnit.HOURS.toMillis(toHour - currentHour - 1) + 
                   TimeUnit.MINUTES.toMillis(60 - currentMinute) + 
                   TimeUnit.MINUTES.toMillis(toMinute), result);
  }
  
  @Test
  public void getDelayTillHourFail() {
    try {
      SchedulingUtils.getDelayTillHour(12, -1);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      SchedulingUtils.getDelayTillHour(12, 60);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      SchedulingUtils.getDelayTillHour(-1, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      SchedulingUtils.getDelayTillHour(24, 10);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
