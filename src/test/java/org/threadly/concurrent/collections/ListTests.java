package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
    
    List<String> toRetainList = new ArrayList<String>(TEST_QTY / 2);
    populateIntStrings(toRetainList, TEST_QTY / 2);
    
    assertTrue(testList.retainAll(toRetainList));
    
    assertEquals(testList.size(), TEST_QTY / 2);
    
    assertTrue(toRetainList.containsAll(testList));
  }
  
  public static void testIterator(List<String> testList) {
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
  }
}
