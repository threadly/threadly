package org.threadly.util;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class SortUtilsTest {
  private static final int BINARY_SEARCH_RANDOM_SIZE = TEST_QTY * 10;
  private static final int INCREMENTAL_SEARCH_COUNT = TEST_QTY * 10;

  @Test
  public void getInsertionEndIndexTest() {
    List<Long> list = new ArrayList<>(10);

    assertEquals(0, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(0, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(0, SortUtils.getInsertionEndIndex(list, 10, true));
    
    list.add(0L);
    assertEquals(1, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(1, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(1, SortUtils.getInsertionEndIndex(list, 10, true));
    
    list.add(0L);
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 10, true));
    
    list.add(10L);
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(3, SortUtils.getInsertionEndIndex(list, 10, true));
    
    list.add(10L);
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(4, SortUtils.getInsertionEndIndex(list, 10, true));
    
    // do one insert in the middle
    list.add(2, 1L);
    assertEquals(2, SortUtils.getInsertionEndIndex(list, 0, true));
    assertEquals(3, SortUtils.getInsertionEndIndex(list, 1, true));
    assertEquals(5, SortUtils.getInsertionEndIndex(list, 10, true));
  }
  
  @Test
  public void binarySearchTest() {
    List<Long> list = new ArrayList<>(BINARY_SEARCH_RANDOM_SIZE + INCREMENTAL_SEARCH_COUNT + 10);
    
    // verify against an empty list
    assertEquals(-1, SortUtils.binarySearch(list, 0, true));
    assertEquals(-1, SortUtils.binarySearch(list, 0, false));
    
    // add one and verify both contains and missing
    list.add(-1L);

    assertEquals(-2, SortUtils.binarySearch(list, 0, true));
    assertEquals(-2, SortUtils.binarySearch(list, 0, false));

    assertEquals(0, SortUtils.binarySearch(list, -1, true));
    assertEquals(0, SortUtils.binarySearch(list, -1, false));
    
    // add one more and verify both contains and missing
    list.add(10L);

    assertEquals(-2, SortUtils.binarySearch(list, 0, true));
    assertEquals(-2, SortUtils.binarySearch(list, 0, false));

    assertEquals(0, SortUtils.binarySearch(list, -1, true));
    assertEquals(0, SortUtils.binarySearch(list, -1, false));

    assertEquals(1, SortUtils.binarySearch(list, 10, true));
    assertEquals(1, SortUtils.binarySearch(list, 10, false));
    
    // add a duplicate entry and verify both contains and missing
    list.add(10L);

    assertEquals(-2, SortUtils.binarySearch(list, 0, true));
    assertEquals(-2, SortUtils.binarySearch(list, 0, false));

    assertEquals(0, SortUtils.binarySearch(list, -1, true));
    assertEquals(0, SortUtils.binarySearch(list, -1, false));
    
    long index = SortUtils.binarySearch(list, 10, false);
    assertTrue(index == 1 || index == 2);
    index = SortUtils.binarySearch(list, 10, true);
    assertTrue(index == 1 || index == 2);
    
    // start above previous tests
    for (long i = 20; i < INCREMENTAL_SEARCH_COUNT; i++) {
      int bsResult = SortUtils.binarySearch(list, i, true);
      assertTrue(bsResult < 0); // should not be in list yet
      int insertionIndex;
      if (bsResult < 0) {
        insertionIndex = Math.abs(bsResult) - 1;
      } else {
        insertionIndex = bsResult;
      }
      list.add(insertionIndex, i);
      assertEquals(insertionIndex, SortUtils.binarySearch(list, i, true));
      assertEquals(insertionIndex, SortUtils.binarySearch(list, i, false));
    }
    
    Random random = new Random(Clock.lastKnownTimeMillis());
    for (int i = 0; i < BINARY_SEARCH_RANDOM_SIZE; i++) {
      long testItem = random.nextInt(BINARY_SEARCH_RANDOM_SIZE);
      int bsResult = SortUtils.binarySearch(list, testItem, true);
      int insertionIndex;
      if (bsResult < 0) {
        insertionIndex = Math.abs(bsResult) - 1;
      } else {
        insertionIndex = bsResult;
      }
      list.add(insertionIndex, testItem);
      
      int raSearch = SortUtils.binarySearch(list, testItem, true);
      int nonraSearch = SortUtils.binarySearch(list, testItem, false);
      assertTrue(raSearch >= 0);
      assertTrue(nonraSearch >= 0);
    }
  }
}
