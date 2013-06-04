package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

@SuppressWarnings("javadoc")
public class ListTests {
  private static final int TEST_QTY = 10;
  
  public static void populateIntStrings(List<String> testList, int qty) {
    for (int i = 0; i < qty; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
  }
  
  public static void sizeTest(List<String> testList) {
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(testList.size(), i);
      testList.add("testStr");
      assertEquals(testList.size(), i + 1);
    }

    for (int i = TEST_QTY; i >= 0; i--) {
      assertEquals(testList.size(), i);
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
  
  public static void addAllTest(List<String> testList) {
    List<String> toAddList = new ArrayList<String>(TEST_QTY);
    populateIntStrings(toAddList, TEST_QTY);
    
    testList.addAll(toAddList);
    
    assertEquals(testList.size(), TEST_QTY);
    assertTrue(testList.containsAll(toAddList));
    
    Iterator<String> it = toAddList.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(it.next() == testIt.next());
    }
  }
  
  public static void addAllIndexTest(List<String> testList) {
    List<String> toAddList = new ArrayList<String>(TEST_QTY);
    testList.add("foo");
    testList.add("bar");
    populateIntStrings(toAddList, TEST_QTY);
    
    testList.addAll(1, toAddList);
    
    assertEquals(testList.size(), TEST_QTY + 2);
    assertTrue(testList.containsAll(toAddList));
    
    assertEquals(testList.indexOf(Integer.toString(0)), 1);
    assertEquals(testList.indexOf(Integer.toString(TEST_QTY - 1)), TEST_QTY);
    
    Iterator<String> it = toAddList.iterator();
    Iterator<String> testIt = testList.iterator();
    assertTrue(testIt.next().equals("foo"));
    while (it.hasNext()) {
      assertTrue(it.next() == testIt.next());
    }
    assertTrue(testIt.next().equals("bar"));
  }
  
  public static void getTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
      assertEquals(testList.get(i), str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(testList.get(i), comparisionList.get(i));
    }
  }
  
  public static void indexOfTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      comparisionList.add(str);
      testList.add(str);
      testList.add(str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertEquals(testList.indexOf(str), comparisionList.indexOf(str));
    }
    
    assertEquals(testList.indexOf("foobar"), -1);
  }
  
  public static void lastIndexOfTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      comparisionList.add(str);
      testList.add(str);
      testList.add(str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertEquals(testList.lastIndexOf(str), comparisionList.lastIndexOf(str));
    }
    
    assertEquals(testList.lastIndexOf("foobar"), -1);
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
    List<String> comparisionList = new ArrayList<String>(TEST_QTY + 1);
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
  }
  
  public static void clearTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    assertEquals(testList.size(), TEST_QTY);
    testList.clear();
    assertEquals(testList.size(), 0);
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
    
    assertEquals(testList.size(), TEST_QTY - removed);
  }
  
  public static void removeAllTest(List<String> testList) {
    List<String> toRemoveList = new ArrayList<String>(TEST_QTY);
    List<String> comparisonList = new ArrayList<String>(TEST_QTY);
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
    
    List<String> noOpList = new ArrayList<String>(2);
    noOpList.add("foo");
    noOpList.add("bar");
    
    assertFalse(testList.removeAll(noOpList));
    
    assertEquals(testList.size(), TEST_QTY);
    
    testList.removeAll(toRemoveList);
    assertEquals(testList.size(), TEST_QTY - toRemoveList.size());
    Iterator<String> it = toRemoveList.iterator();
    while (it.hasNext()) {
      assertFalse(testList.contains(it.next()));
    }
    
    comparisonList.removeAll(toRemoveList);  // do operation on comparison list
    assertTrue(testList.containsAll(comparisonList));  // verify nothing additional was removed
  }
  
  public static void removeIndexTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    List<String> removedItems = new LinkedList<String>();
    String removedItem = testList.remove(TEST_QTY - 1);
    assertTrue(removedItem.equals(Integer.toString(TEST_QTY - 1)));
    removedItems.add(removedItem);
    
    removedItem = testList.remove(TEST_QTY / 2);
    assertTrue(removedItem.equals(Integer.toString(TEST_QTY / 2)));
    removedItems.add(removedItem);
    
    removedItem = testList.remove(0);
    assertTrue(removedItem.equals(Integer.toString(0)));
    removedItems.add(removedItem);
    
    
    Iterator<String> it = removedItems.iterator();
    while (it.hasNext()) {
      assertFalse(testList.contains(it.next()));
    }
  }
  
  public static void retainAllTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    assertTrue(testList.retainAll(new ArrayList<String>(0)));
    
    assertEquals(testList.size(), 0);
    
    populateIntStrings(testList, TEST_QTY);
    
    assertFalse(testList.retainAll(testList));
    assertFalse(testList.retainAll(new ArrayList<String>(testList)));
    
    List<String> toRetainList = new ArrayList<String>(TEST_QTY / 2);
    populateIntStrings(toRetainList, TEST_QTY / 2);
    
    assertTrue(testList.retainAll(toRetainList));
    
    assertEquals(testList.size(), TEST_QTY / 2);
    
    assertTrue(toRetainList.containsAll(testList));
  }
  
  public static void iteratorTest(List<String> testList) {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
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
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
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
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
      assertTrue(testList.equals(comparisionList));
    }
    
    String foo = "foo";
    comparisionList.add(foo);
    assertFalse(testList.equals(comparisionList));
    
    testList.add(foo);
    assertTrue(testList.equals(comparisionList));
    
    testList.add(foo);
    assertFalse(testList.equals(comparisionList));
    
    assertEquals(testList.subList(0, testList.size()).hashCode(), testList.hashCode());
  }
  
  public static void subListTest(List<String> testList) {
    populateIntStrings(testList, TEST_QTY);
    
    List<String> completeList = testList.subList(0, TEST_QTY); 
    assertEquals(completeList.size(), TEST_QTY);
    Iterator<String> it1 = testList.iterator();
    Iterator<String> it2 = completeList.iterator();
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(it1.next(), it2.next());
    }
    
    List<String> smallList = testList.subList(0, 1);
    assertEquals(smallList.size(), 1);
    assertEquals(smallList.get(0), testList.get(0));
    
    smallList = testList.subList(TEST_QTY - 1, TEST_QTY);
    assertEquals(smallList.size(), 1);
    assertEquals(smallList.get(0), testList.get(TEST_QTY - 1));
    
    int halfQty = TEST_QTY / 2;
    List<String> mediumList = testList.subList(1, halfQty);
    assertEquals(mediumList.size(), halfQty - 1);
    it1 = mediumList.iterator();
    for (int i = 1; i < halfQty; i++) {
      assertEquals(it1.next(), testList.get(i));
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
  }
}
