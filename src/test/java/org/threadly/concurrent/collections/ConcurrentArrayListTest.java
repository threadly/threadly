package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.collections.DynamicDelayQueueTest.TestDelayed;
import org.threadly.concurrent.lock.NativeLock;
import org.threadly.concurrent.lock.VirtualLock;

@SuppressWarnings("javadoc")
public class ConcurrentArrayListTest {
  private static final int TEST_QTY = 10;
  
  private ConcurrentArrayList<String> testList;
  
  @Before
  public void setup() {
    testList = new ConcurrentArrayList<String>();
  }
  
  @After
  public void tearDown() {
    testList = null;
  }
  
  @Test
  public void getModificationLockTest() {
    VirtualLock testLock = new NativeLock();
    ConcurrentArrayList<String> testList = new ConcurrentArrayList<String>(testLock);
    
    assertTrue(testLock == testList.getModificationLock());
  }
  
  @Test
  public void setFrontPaddingTest() {
    testList.setFrontPadding(1);
    assertEquals(testList.getFrontPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getFrontPadding(), 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void setFrontPaddingFail() {
    testList.setFrontPadding(-1);
  }
  
  @Test
  public void setRearPaddingTest() {
    testList.setRearPadding(1);
    assertEquals(testList.getRearPadding(), 1);
    
    // make some modifications
    testList.add("foo");
    testList.add("bar");
    testList.remove(0);
    
    assertEquals(testList.getRearPadding(), 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setRearPaddingFail() {
    testList.setRearPadding(-1);
  }

  @Test
  public void sizeTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      assertEquals(testList.size(), i);
      testList.add("testStr");
      assertEquals(testList.size(), i + 1);
    }

    for (int i = TEST_QTY; i >= 0; i--) {
      assertEquals(testList.size(), i);
      if (i != 0) {
        testList.removeFirst();
      }
    }
  }

  @Test
  public void isEmptyTest() {
    assertTrue(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.add("foo");
    assertFalse(testList.isEmpty());
    testList.removeFirst();
    assertFalse(testList.isEmpty());
    testList.removeFirst();
    assertTrue(testList.isEmpty());
  }
  
  @Test
  public void getTest() {
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
  
  @Test
  public void indexOfTest() {
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
  
  @Test
  public void lastIndexOfTest() {
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
  
  @Test
  public void containsTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.contains(str));
    }
    for (int i = TEST_QTY + 1; i < TEST_QTY * 2; i++) {
      String str = Integer.toString(i);
      assertFalse(testList.contains(str));
    }
  }
  
  @Test
  public void containsAllTest() {
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
  
  @Test
  public void toArrayTest() {
    assertArrayEquals(testList.toArray(), new String[0]);
    
    String[] compare = new String[TEST_QTY];
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compare[i] = str;
      testList.add(str);
    }
    
    assertArrayEquals(testList.toArray(), compare);
  }
  
  @Test
  public void clearTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
    assertEquals(testList.size(), TEST_QTY);
    testList.clear();
    assertEquals(testList.size(), 0);
    assertNull(testList.peek());
  }
  
  @Test
  public void addFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addFirst(str);
      assertEquals(testList.getFirst(), str);
    }
  }
  
  @Test
  public void addLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(testList.getLast(), str);
    }
  }
  
  @Test
  public void peekFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addFirst(str);
      assertEquals(testList.peekFirst(), str);
    }
  }
  
  @Test
  public void peekLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(testList.peekLast(), str);
    }
  }
  
  @Test
  public void removeAllTest() {
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
  
  @Test
  public void removeFirstOccurrenceTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      testList.add(str1);
      testList.add(str2);
    }
    
    
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.removeFirstOccurrence(str);
    }
    
    assertEquals(testList.size(), secondStr.size());
    
    Iterator<String> it = secondStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  @Test
  public void removeLastOccurrenceTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      testList.add(str1);
      testList.add(str2);
    }
    
    
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.removeLastOccurrence(str);
    }
    
    assertEquals(testList.size(), firstStr.size());
    
    Iterator<String> it = firstStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  public void removeIndexTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
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
  
  /*@Test
  public void repositionSearchForwardTest() {
    // TODO - implement
  }
  
  @Test
  public void repositionSearchBackwardTest() {
    // TODO - implement
  }
  
  @Test
  public void repositionIndexTest() {
    // TODO - implement
  }*/
  
  /* This also tests the ListIterator forwards, 
   * since this just defaults to that implementation
   */
  @Test
  public void testIterator() {
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
  
  @Test
  public void testListIteratorBackwards() {
    List<String> comparisionList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionList.add(str);
      testList.add(str);
    }
    
    ListIterator<String> clIt = comparisionList.listIterator(TEST_QTY);
    ListIterator<String> testIt = testList.listIterator(TEST_QTY);
    while (clIt.hasPrevious()) {
      assertTrue(testIt.hasPrevious());
      assertEquals(clIt.previous(), testIt.previous());
    }
  }
  
  @Test
  public void descendingIteratorTest() {
    Deque<String> comparisionDeque = new LinkedList<String>();
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      comparisionDeque.addLast(str);
      testList.add(str);
    }
    
    Iterator<String> clIt = comparisionDeque.descendingIterator();
    Iterator<String> testIt = testList.descendingIterator();
    while (clIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(clIt.next(), testIt.next());
    }
  }
}
