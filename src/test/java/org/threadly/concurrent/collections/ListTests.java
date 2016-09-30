package org.threadly.concurrent.collections;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

@SuppressWarnings("javadoc")
public class ListTests {
  public static void populateIntStrings(List<String> testList, int qty) {
    for (int i = 0; i < qty; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
  }
  
  public static void sizeTest(List<String> testList) {
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(i, testList.size());
      testList.add("testStr");
      assertEquals(i + 1, testList.size());
    }

    for (int i = TEST_QTY; i >= 0; i--) {
      assertEquals(i, testList.size());
      if (i != 0) {
        testList.remove(0);
      }
    }
  }
  
  public static void isEmptyTest(List<String> testList) {
    assertTrue(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.remove(0);
    assertFalse(testList.isEmpty());
    testList.remove(0);
    assertTrue(testList.isEmpty());
  }
  
  public static void addNullTest(List<String> testList) {
    assertFalse(testList.add(null));
  }
  
  public static void addAllTest(List<String> testList) {
    List<String> toAddList = new ArrayList<>(TEST_QTY);
    
    // verify with empty list first
    assertFalse(testList.addAll(toAddList));
    
    populateIntStrings(toAddList, TEST_QTY);
    
    testList.addAll(toAddList);
    
    assertEquals(TEST_QTY, testList.size());
    assertTrue(testList.containsAll(toAddList));
    
    Iterator<String> it = toAddList.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(it.next() == testIt.next());
    }
  }
  
  public static void setFail(List<String> testList) {
    try {
      testList.set(1, "foo");
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.set(-1, "foo");
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  public static void addIndexTest(List<String> testList) {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(i, str);
    }
    
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(Integer.toString(i), testList.get(i));
    }
    
    // add a second set of items to the front
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(i, str);
    }
    
    // add a third set of items to the end
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(testList.size(), str);
    }
    
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < TEST_QTY; j++) {
        int testIndex = j + (TEST_QTY * i);
        assertEquals(Integer.toString(j), testList.get(testIndex));
      }
    }
  }
  
  public static void addIndexFail(List<String> testList) {
    try {
      testList.add(1, "foo");
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.add(-1, "foo");
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  public static void addAllIndexTest(List<String> testList) {
    List<String> toAddList = new ArrayList<>(TEST_QTY);
    
    // verify with empty list first
    assertFalse(testList.addAll(0, toAddList));
    
    testList.add("foo");
    testList.add("bar");
    populateIntStrings(toAddList, TEST_QTY);
    
    testList.addAll(1, toAddList);
    
    assertEquals(TEST_QTY + 2, testList.size());
    assertTrue(testList.containsAll(toAddList));
    
    assertEquals(1, testList.indexOf(Integer.toString(0)));
    assertEquals(TEST_QTY, testList.indexOf(Integer.toString(TEST_QTY - 1)));
    
    Iterator<String> it = toAddList.iterator();
    Iterator<String> testIt = testList.iterator();
    assertTrue(testIt.next().equals("foo"));
    while (it.hasNext()) {
      assertTrue(it.next() == testIt.next());
    }
    assertTrue(testIt.next().equals("bar"));
  }
  
  public static void addAllIndexFail(List<String> testList) {
    List<String> toAdd = new ArrayList<>(1);
    toAdd.add("foo");
    
    try {
      testList.addAll(1, toAdd);
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.addAll(-1, toAdd);
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  public static void getTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
      assertEquals(str, testList.get(i));
    }
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(comparisionList.get(i), testList.get(i));
    }
  }
  
  public static void indexOfTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      comparisionList.add(str);
      testList.add(str);
      testList.add(str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertEquals(comparisionList.indexOf(str), testList.indexOf(str));
    }
    
    assertEquals(-1, testList.indexOf("foobar"));
  }
  
  public static void lastIndexOfTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      comparisionList.add(str);
      testList.add(str);
      testList.add(str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertEquals(comparisionList.lastIndexOf(str), testList.lastIndexOf(str));
    }
    
    assertEquals(-1, testList.lastIndexOf("foobar"));
  }
  
  public static void containsTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.contains(str));
    }
    for (int i = TEST_QTY + 1; i < TEST_QTY * 2; i++) {
      String str = Integer.toString(i);
      assertFalse(testList.contains(str));
    }
  }
  
  public static void containsAllTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY + 1);
    assertTrue(testList.containsAll(comparisionList));
    comparisionList.add("bar");
    assertFalse(testList.containsAll(comparisionList));
    testList.add("bar");
    assertTrue(testList.containsAll(comparisionList));
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      assertFalse(testList.containsAll(comparisionList));
      testList.add(str);
      assertTrue(testList.containsAll(comparisionList));
    }
    
    testList.add("foobar");
    assertTrue(testList.containsAll(comparisionList));
  }
  
  public static void toArrayTest(List<String> testList) {
    assertArrayEquals(testList.toArray(), new String[0]);
    
    String[] compare = new String[TEST_QTY];
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compare[i] = str;
      testList.add(str);
    }
    
    assertArrayEquals(testList.toArray(), compare);
    
    String[] expectedResult = compare;
    
    // reset for next test
    compare = new String[TEST_QTY];
    String[] result = testList.toArray(compare);
    
    assertArrayEquals(result, expectedResult);
    assertTrue(result == compare);
    
    result = testList.toArray(new String[0]);
    assertArrayEquals(result, expectedResult);
  }
  
  public static void clearTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    assertEquals(TEST_QTY, testList.size());
    testList.clear();
    assertEquals(0, testList.size());
  }
  
  public static void removeMissingObjectTest(List<String> testList) {
    assertFalse(testList.remove(new Object()));
    assertFalse(testList.remove("foo"));
  }
  
  public static void removeObjectTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    boolean flip = false;
    int removed = 0;
    for (int i = 0; i < TEST_QTY; i++) {
      if (flip) {
        String str = Integer.toString(i);
        assertTrue(testList.remove(str));
        assertFalse(testList.contains(str));
        removed++;
        flip = false;
      } else {
        flip = true;
      }
    }
    
    assertEquals(TEST_QTY - removed, testList.size());
  }
  
  public static void removeAllTest(List<String> testList) {
    List<String> toRemoveList = new ArrayList<>(TEST_QTY);
    
    // verify returned false on no modification
    assertFalse(testList.removeAll(toRemoveList));
    
    List<String> comparisonList = new ArrayList<>(TEST_QTY);
    boolean flip = false;
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
      comparisonList.add(str);
      if (flip) {
        toRemoveList.add(str);
        flip = false;
      } else {
        flip = true;
      }
    }
    
    List<String> noOpList = new ArrayList<>(2);
    noOpList.add("foo");
    noOpList.add("bar");
    
    assertFalse(testList.removeAll(noOpList));
    
    assertEquals(TEST_QTY, testList.size());
    
    assertTrue(testList.removeAll(toRemoveList));
    assertEquals(TEST_QTY - toRemoveList.size(), testList.size());
    Iterator<String> it = toRemoveList.iterator();
    while (it.hasNext()) {
      assertFalse(testList.contains(it.next()));
    }
    
    comparisonList.removeAll(toRemoveList);  // do operation on comparison list
    assertTrue(testList.containsAll(comparisonList));  // verify nothing additional was removed
  }
  
  public static void removeIndexTest(List<String> testList) {
    int testQty = Math.max(4, TEST_QTY);
    
    populateIntStrings(testList, testQty);
    
    List<String> removedItems = new LinkedList<>();
    String removedItem = testList.remove(testQty - 1);
    assertTrue(removedItem.equals(Integer.toString(testQty - 1)));
    removedItems.add(removedItem);
    
    removedItem = testList.remove(testQty / 2);
    assertTrue(removedItem.equals(Integer.toString(testQty / 2)));
    removedItems.add(removedItem);
    
    removedItem = testList.remove(0);
    assertTrue(removedItem.equals(Integer.toString(0)));
    removedItems.add(removedItem);
    
    Iterator<String> it = removedItems.iterator();
    while (it.hasNext()) {
      assertFalse(testList.contains(it.next()));
    }
  }
  
  public static void removeIndexFail(List<String> testList) {
    try {
      testList.remove(1);
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.remove(-1);
      fail("Exception should have thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  public static void retainAllTest(List<String> testList) {
    // verify with empty list
    assertFalse(testList.retainAll(Collections.emptyList()));
    
    populateIntStrings(testList, TEST_QTY);
    
    assertTrue(testList.retainAll(Collections.emptyList()));
    
    assertEquals(0, testList.size());
    
    populateIntStrings(testList, TEST_QTY);
    
    assertFalse(testList.retainAll(testList));
    assertFalse(testList.retainAll(new ArrayList<>(testList)));
    
    List<String> toRetainList = new ArrayList<>(TEST_QTY / 2);
    populateIntStrings(toRetainList, TEST_QTY / 2);
    
    assertTrue(testList.retainAll(toRetainList));
    
    assertEquals(TEST_QTY / 2, testList.size());
    
    assertTrue(toRetainList.containsAll(testList));
  }
  
  public static void iteratorTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
    }
    
    Iterator<String> clIt = comparisionList.iterator();
    Iterator<String> testIt = testList.iterator();
    while (clIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(clIt.next(), testIt.next());
    }
    
    boolean flip = false;
    clIt = comparisionList.iterator();
    testIt = testList.iterator();
    while (clIt.hasNext()) {
      clIt.next();
      testIt.next();
      if (flip) {
        clIt.remove();
        testIt.remove();
        
        flip = false;
      } else {
        flip = true;
      }
    }
    
    assertTrue(comparisionList.equals(testList));
  }
  
  public static void listIteratorTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
    }
    
    // forward
    ListIterator<String> clIt = comparisionList.listIterator(0);
    ListIterator<String> testIt = testList.listIterator(0);
    assertFalse(testIt.hasPrevious());
    while (clIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(clIt.next(), testIt.next());
      assertEquals(clIt.nextIndex(), testIt.nextIndex());
      //assertEquals(clIt.previousIndex(), testIt.previousIndex());
    }
    
    // backwards
    clIt = comparisionList.listIterator(comparisionList.size());
    testIt = testList.listIterator(testList.size());
    assertFalse(testIt.hasNext());
    while (clIt.hasPrevious()) {
      assertTrue(testIt.hasPrevious());
      assertEquals(clIt.previous(), testIt.previous());
      assertEquals(clIt.nextIndex(), testIt.nextIndex());
      assertEquals(clIt.previousIndex(), testIt.previousIndex());
    }
    
    // modify
    int iteration = Integer.MAX_VALUE;
    boolean flip = false;
    clIt = comparisionList.listIterator();
    testIt = testList.listIterator();
    while (clIt.hasNext()) {
      iteration--;
      clIt.next();
      testIt.next();
      if (flip) {
        clIt.remove();
        testIt.remove();
        
        flip = false;
      } else {
        String value = Integer.toHexString(iteration);
        clIt.add(value);
        testIt.add(value);
        flip = true;
      }
    }
    assertTrue(comparisionList.equals(testList));
  }
  
  public static void listIteratorFail(List<String> testList) {
    ListIterator<String> it = testList.listIterator();
    try {
      it.next();
      fail("Exception should have been thrown");
    } catch (NoSuchElementException e) {
      // expected
    }
    it = testList.listIterator();
    try {
      it.previous();
      fail("Exception should have been thrown");
    } catch (NoSuchElementException e) {
      // expected
    }
    
    populateIntStrings(testList, TEST_QTY);
    
    it = testList.listIterator();
    try {
      it.previous();
      fail("Exception should have been thrown");
    } catch (NoSuchElementException e) {
      // expected
    }
    it = testList.listIterator(testList.size());
    try {
      it.next();
      fail("Exception should have been thrown");
    } catch (NoSuchElementException e) {
      // expected
    }
  }
  
  public static void equalsTest(List<String> testList) {
    assertFalse(testList.equals(new Object()));
    
    List<String> comparisionList = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
      assertTrue(testList.equals(comparisionList));
    }
    // easy test
    assertTrue(testList.equals(testList));
    
    String foo = "foo";
    comparisionList.add(foo);
    assertFalse(testList.equals(comparisionList));
    
    testList.add(foo);
    assertTrue(testList.equals(comparisionList));
    
    testList.add(foo);
    assertFalse(testList.equals(comparisionList));
    
    assertEquals(testList.hashCode(), testList.subList(0, testList.size()).hashCode());
  }
  
  public static void subListTest(List<String> testList) {
    int testQty = Math.max(4, TEST_QTY);
    
    populateIntStrings(testList, testQty);
    
    List<String> completeList = testList.subList(0, testQty); 
    assertEquals(testQty, completeList.size());
    Iterator<String> it1 = testList.iterator();
    Iterator<String> it2 = completeList.iterator();
    for (int i = 0; i < testQty; i++) {
      assertEquals(it1.next(), it2.next());
    }
    
    List<String> smallList = testList.subList(0, 1);
    assertEquals(1, smallList.size());
    assertEquals(testList.get(0), smallList.get(0));
    
    smallList = testList.subList(testQty - 1, testQty);
    assertEquals(1, smallList.size());
    assertEquals(testList.get(testQty - 1), smallList.get(0));
    
    int halfQty = testQty / 2;
    List<String> mediumList = testList.subList(1, halfQty);
    assertEquals(mediumList.size(), halfQty - 1);
    it1 = mediumList.iterator();
    for (int i = 1; i < halfQty; i++) {
      assertEquals(testList.get(i), it1.next());
    }
  }
  
  public static void subListFail(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    try {
      testList.subList(-1, 0);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.subList(0, -1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.subList(2, 0);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.subList(0, TEST_QTY + 1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.subList(TEST_QTY + 10, TEST_QTY);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
}
