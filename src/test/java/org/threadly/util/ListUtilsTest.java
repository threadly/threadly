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
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 0);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 0);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 0);
    
    list.add(list.size(), zero);
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 1);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 1);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 1);
    
    list.add(list.size(), zero);
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 2);
    
    list.add(list.size(), ten);
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 3);
    
    list.add(list.size(), ten);
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 4);
    
    // do one insert in the middle
    list.add(2, one);
    assertTrue(ListUtils.getInsertionEndIndex(list, zero) == 2);
    assertTrue(ListUtils.getInsertionEndIndex(list, one) == 3);
    assertTrue(ListUtils.getInsertionEndIndex(list, ten) == 5);
  }
  
  @Test
  public void binarySearchTest() {
    List<TestDelayed> list = new ArrayList<TestDelayed>(BINARY_SEARCH_RANDOM_SIZE + 
                                                          INCREMENTAL_SEARCH_COUNT + 10);
    
    TestDelayed zero = new TestDelayed(0);
    
    // verify against an empty list
    assertTrue(ListUtils.binarySearch(list, zero) == -1);
    assertTrue(ListUtils.binarySearch(list, zero, false) == -1);
    assertTrue(ListUtils.binarySearch(list, zero, true) == -1);
    
    // add one and verify both contains and missing
    TestDelayed negativeOne = new TestDelayed(-1);
    list.add(negativeOne);

    assertTrue(ListUtils.binarySearch(list, zero) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, false) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, true) == -2);

    assertTrue(ListUtils.binarySearch(list, negativeOne) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, false) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, true) == 0);
    
    // add one more and verify both contains and missing
    TestDelayed ten = new TestDelayed(10);
    list.add(ten);

    assertTrue(ListUtils.binarySearch(list, zero) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, false) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, true) == -2);

    assertTrue(ListUtils.binarySearch(list, negativeOne) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, false) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, true) == 0);
    
    assertTrue(ListUtils.binarySearch(list, ten) == 1);
    assertTrue(ListUtils.binarySearch(list, ten, false) == 1);
    assertTrue(ListUtils.binarySearch(list, ten, true) == 1);
    
    // add a duplicate entry and verify both contains and missing
    list.add(ten);

    assertTrue(ListUtils.binarySearch(list, zero) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, false) == -2);
    assertTrue(ListUtils.binarySearch(list, zero, true) == -2);

    assertTrue(ListUtils.binarySearch(list, negativeOne) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, false) == 0);
    assertTrue(ListUtils.binarySearch(list, negativeOne, true) == 0);
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
      assertTrue(ListUtils.binarySearch(list, testItem, true) == insertionIndex);
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
