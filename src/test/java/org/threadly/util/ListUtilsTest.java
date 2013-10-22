package org.threadly.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.threadly.concurrent.TestDelayed;

@SuppressWarnings("javadoc")
public class ListUtilsTest {
  private static final int BINARY_SEARCH_RANDOM_SIZE = 100;
  private static final int INCREMENTAL_SEARCH_COUNT = 100;
  
  @Test
  public void getInsertionEndIndexTest() {
    List<TestDelayed> list = new ArrayList<TestDelayed>(10);

    TestDelayed zero = new TestDelayed(0);
    TestDelayed one = new TestDelayed(1);
    TestDelayed ten = new TestDelayed(10);
    assertEquals(0, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(0, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(0, ListUtils.getInsertionEndIndex(list, ten));
    
    list.add(list.size(), zero);
    assertEquals(1, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(1, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(1, ListUtils.getInsertionEndIndex(list, ten));
    
    list.add(list.size(), zero);
    assertEquals(2, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(2, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(2, ListUtils.getInsertionEndIndex(list, ten));
    
    list.add(list.size(), ten);
    assertEquals(2, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(2, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(3, ListUtils.getInsertionEndIndex(list, ten));
    
    list.add(list.size(), ten);
    assertEquals(2, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(2, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(4, ListUtils.getInsertionEndIndex(list, ten));
    
    // do one insert in the middle
    list.add(2, one);
    assertEquals(2, ListUtils.getInsertionEndIndex(list, zero));
    assertEquals(3, ListUtils.getInsertionEndIndex(list, one));
    assertEquals(5, ListUtils.getInsertionEndIndex(list, ten));
  }
  
  @Test
  public void binarySearchTest() {
    List<TestDelayed> list = new ArrayList<TestDelayed>(BINARY_SEARCH_RANDOM_SIZE + 
                                                          INCREMENTAL_SEARCH_COUNT + 10);
    
    TestDelayed zero = new TestDelayed(0);
    
    // verify against an empty list
    assertEquals(-1, ListUtils.binarySearch(list, zero));
    assertEquals(-1, ListUtils.binarySearch(list, zero, false));
    assertEquals(-1, ListUtils.binarySearch(list, zero, true));
    
    // add one and verify both contains and missing
    TestDelayed negativeOne = new TestDelayed(-1);
    list.add(negativeOne);

    assertEquals(-2, ListUtils.binarySearch(list, zero));
    assertEquals(-2, ListUtils.binarySearch(list, zero, false));
    assertEquals(-2, ListUtils.binarySearch(list, zero, true));

    assertEquals(0, ListUtils.binarySearch(list, negativeOne));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, false));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, true));
    
    // add one more and verify both contains and missing
    TestDelayed ten = new TestDelayed(10);
    list.add(ten);

    assertEquals(-2, ListUtils.binarySearch(list, zero));
    assertEquals(-2, ListUtils.binarySearch(list, zero, false));
    assertEquals(-2, ListUtils.binarySearch(list, zero, true));

    assertEquals(0, ListUtils.binarySearch(list, negativeOne));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, false));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, true));
    
    assertEquals(1, ListUtils.binarySearch(list, ten));
    assertEquals(1, ListUtils.binarySearch(list, ten, false));
    assertEquals(1, ListUtils.binarySearch(list, ten, true));
    
    // add a duplicate entry and verify both contains and missing
    list.add(ten);

    assertEquals(-2, ListUtils.binarySearch(list, zero));
    assertEquals(-2, ListUtils.binarySearch(list, zero, false));
    assertEquals(-2, ListUtils.binarySearch(list, zero, true));

    assertEquals(0, ListUtils.binarySearch(list, negativeOne));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, false));
    assertEquals(0, ListUtils.binarySearch(list, negativeOne, true));
    long index = ListUtils.binarySearch(list, ten, false);
    assertTrue(index == 1 || index == 2);
    index = ListUtils.binarySearch(list, ten, true);
    assertTrue(index == 1 || index == 2);
    
    // start above previous tests
    for (int i = 20; i < INCREMENTAL_SEARCH_COUNT; i++) {
      TestDelayed testItem = new TestDelayed(i);
      int bsResult = ListUtils.binarySearch(list, testItem, true);
      assertTrue(bsResult < 0); // should not be in list yet
      int insertionIndex;
      if (bsResult < 0) {
        insertionIndex = Math.abs(bsResult) - 1;
      } else {
        insertionIndex = bsResult;
      }
      list.add(insertionIndex, testItem);
      assertEquals(insertionIndex, ListUtils.binarySearch(list, testItem, true));
    }
    
    Random random = new Random(System.currentTimeMillis());
    for (int i = 0; i < BINARY_SEARCH_RANDOM_SIZE; i++) {
      TestDelayed testItem = new TestDelayed(random.nextInt(BINARY_SEARCH_RANDOM_SIZE));
      int bsResult = ListUtils.binarySearch(list, testItem, true);
      int insertionIndex;
      if (bsResult < 0) {
        insertionIndex = Math.abs(bsResult) - 1;
      } else {
        insertionIndex = bsResult;
      }
      list.add(insertionIndex, testItem);
      
      int raSearch = ListUtils.binarySearch(list, testItem, true);
      int nonraSearch = ListUtils.binarySearch(list, testItem, false);
      assertTrue(raSearch >= 0);
      assertTrue(nonraSearch >= 0);
    }
  }
}
