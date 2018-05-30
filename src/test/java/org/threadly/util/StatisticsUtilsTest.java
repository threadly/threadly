package org.threadly.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class StatisticsUtilsTest extends ThreadlyTester {
  @Test
  public void averageTest() {
    assertEquals(0, StatisticsUtils.getAverage(Collections.<Long>emptyList()), 0);
    assertEquals(2, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ 2 })), 0);
    assertEquals(2, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ 2, 2 })), 0);
    assertEquals(2, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ 1, 3 })), 0);
    assertEquals(2, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ 1, 3, 1, 3 })), 0);
    assertEquals(5, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ 0, 10 })), 0);
    assertEquals(0, StatisticsUtils.getAverage(Arrays.asList(new Integer[]{ -10, 10 })), 0);
  }
  
  @Test
  public void percentileTest() {
    Map<Double, Integer> pResult = 
        StatisticsUtils.getPercentiles(Arrays.asList(new Integer[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }), 
                                       0, 50, 99, 100);
    assertEquals(1, pResult.get(0.).intValue());
    assertEquals(6, pResult.get(50.).intValue());
    assertEquals(10, pResult.get(99.).intValue());
    assertEquals(10, pResult.get(100.).intValue());
  }
  
  @Test
  public void percentileFail() {
    try {
      StatisticsUtils.getPercentiles(Collections.<Long>emptyList(), 50, 99);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      StatisticsUtils.getPercentiles(Arrays.asList(new Integer[]{ 0, 10 }));
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
