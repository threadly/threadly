package org.threadly.concurrent.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.collections.ConcurrentArrayList;
import org.threadly.concurrent.collections.ConcurrentArrayList.DataSet;
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
    testList.offer("bar");
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
    testList.offer("bar");
    testList.remove(0);
    
    assertEquals(testList.getRearPadding(), 1);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void setRearPaddingFail() {
    testList.setRearPadding(-1);
  }

  @Test
  public void sizeTest() {
    ListTests.sizeTest(testList);
  }

  @Test
  public void isEmptyTest() {
    ListTests.isEmptyTest(testList);
  }
  
  @Test
  public void getTest() {
    ListTests.getTest(testList);
  }
  
  @Test
  public void indexOfTest() {
    ListTests.indexOfTest(testList);
  }
  
  @Test
  public void lastIndexOfTest() {
    ListTests.lastIndexOfTest(testList);
  }
  
  @Test
  public void containsTest() {
    ListTests.containsTest(testList);
  }
  
  @Test
  public void containsAllTest() {
    ListTests.containsAllTest(testList);
  }
  
  @Test
  public void toArrayTest() {
    ListTests.toArrayTest(testList);
  }
  
  @Test
  public void clearTest() {
    ListTests.clearTest(testList);
    
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
  public void offerFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.offerFirst(str));
      assertEquals(testList.getFirst(), str);
    }
  }
  
  @Test
  public void offerLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.offerLast(str));
      assertEquals(testList.getLast(), str);
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void getFirstFail() {
    testList.getFirst();
  }
  
  @Test (expected = NoSuchElementException.class)
  public void getLastFail() {
    testList.getLast();
  }
  
  @Test
  public void addAllTest() {
    ListTests.addAllTest(testList);
  }
  
  @Test
  public void addAllIndexTest() {
    ListTests.addAllIndexTest(testList);
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
    ListTests.removeAllTest(testList);
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
      assertTrue(testList.offer(str2));
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
      assertTrue(testList.offer(str2));
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
  
  @Test
  public void removeFirstTest() {
    List<String> compareList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compareList.add(str);
      testList.add(str);
    }
    
    Iterator<String> it = compareList.iterator();
    int removed = 0;
    while (it.hasNext()) {
      String next = it.next();
      assertTrue(testList.removeFirst() == next);
      removed++;
      assertEquals(testList.size(), TEST_QTY - removed);
      assertFalse(testList.peek() == next);
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void removeFirstFail() {
    testList.removeFirst();
  }
  
  @Test
  public void removeLastTest() {
    LinkedList<String> compareList = new LinkedList<String>();
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compareList.add(str);
      testList.add(str);
    }
    
    Iterator<String> it = compareList.descendingIterator();
    int removed = 0;
    while (it.hasNext()) {
      String next = it.next();
      assertTrue(testList.removeLast() == next);
      removed++;
      assertEquals(testList.size(), TEST_QTY - removed);
      assertFalse(testList.peekLast() == next);
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void removeLastFail() {
    testList.removeLast();
  }
  
  @Test
  public void pollFirstTest() {
    List<String> compareList = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compareList.add(str);
      testList.add(str);
    }
    
    Iterator<String> it = compareList.iterator();
    int removed = 0;
    while (it.hasNext()) {
      String next = it.next();
      assertTrue(testList.pollFirst() == next);
      removed++;
      assertEquals(testList.size(), TEST_QTY - removed);
      assertFalse(testList.peek() == next);
    }
  }
  
  @Test
  public void pollLastTest() {
    LinkedList<String> compareList = new LinkedList<String>();
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      compareList.add(str);
      testList.add(str);
    }
    
    Iterator<String> it = compareList.descendingIterator();
    int removed = 0;
    while (it.hasNext()) {
      String next = it.next();
      assertTrue(testList.pollLast() == next);
      removed++;
      assertEquals(testList.size(), TEST_QTY - removed);
      assertFalse(testList.peekLast() == next);
    }
  }
  
  @Test
  public void removeObjectTest() {
    ListTests.removeObjectTest(testList);
  }
  
  @Test
  public void removeIndexTest() {
    ListTests.removeIndexTest(testList);
  }
  
  @Test
  public void retainAllTest() {
    ListTests.retainAllTest(testList);
  }
  
  @Test
  public void repositionSearchForwardTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      assertTrue(testList.offer(str1));
      testList.add(str2);
    }

    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.reposition(str, i, false);
    }
    
    Iterator<String> fIt = firstStr.iterator();
    Iterator<String> sIt = secondStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (fIt.hasNext()) {
      String next = testIt.next();
      assertTrue(fIt.next() == next);
      assertFalse(sIt.next() == next);
    }
  }
  
  @Test
  public void repositionSearchBackwardTest() {
    List<String> firstStr = new ArrayList<String>(TEST_QTY);
    List<String> secondStr = new ArrayList<String>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str1 = Integer.toString(i);
      firstStr.add(str1);
      String str2 = Integer.toString(i);
      secondStr.add(str2);
      assertTrue(testList.offer(str1));
      testList.add(str2);
    }

    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.reposition(str, i, true);
    }
    
    Iterator<String> fIt = firstStr.iterator();
    Iterator<String> sIt = secondStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (sIt.hasNext()) {
      String next = testIt.next();
      assertTrue(sIt.next() == next);
      assertFalse(fIt.next() == next);
    }
  }

  
  @Test (expected = IndexOutOfBoundsException.class)
  public void repositionObjectIndexFail() {
    testList.reposition(Integer.toString(0), 1);
    fail("Exception should have been thrown");
  }
  
  @Test (expected = NoSuchElementException.class)
  public void repositionObjectNotFoundFail() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
    testList.reposition("foobar", 0);
    fail("Exception should have been thrown");
  }
  
  @Test
  public void repositionIndexTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
    testList.reposition(TEST_QTY - 1, 0);
    assertEquals(testList.get(0), Integer.toString(TEST_QTY - 1));
    assertEquals(testList.get(1), Integer.toString(0));

    String expectedNext = testList.get(6);
    testList.reposition(0, 5);
    assertEquals(testList.get(4), // one less than index position because shifted right 
                 Integer.toString(TEST_QTY - 1));
    assertEquals(testList.get(6), expectedNext);
    
    testList.reposition(1, 3);  // swap 1 to 2
    assertEquals(testList.get(1), Integer.toString(2));
    assertEquals(testList.get(2), Integer.toString(1));
  }
  
  /* This also tests the ListIterator forwards, 
   * since this just defaults to that implementation
   */
  @Test
  public void iteratorTest() {
    ListTests.iteratorTest(testList);
  }
  
  @Test
  public void listIteratorTest() {
    ListTests.listIteratorTest(testList);
  }
  
  @Test
  public void listIteratorFail() {
    ListTests.listIteratorFail(testList);
  }
  
  @Test
  public void equalsTest() {
    ListTests.equalsTest(testList);
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
  
  @Test
  public void subListTest() {
    ListTests.subListTest(testList);
  }
  
  @Test
  public void subListFail() {
    ListTests.subListFail(testList);
  }
  
  @Test
  public void makeEmptyDataSetTest() {
    DataSet<String> ds = ConcurrentArrayList.makeEmptyDataSet(0, 0);
    
    assertEquals(ds.size, 0);
    
    ds = ConcurrentArrayList.makeEmptyDataSet(10, 10);
    assertEquals(ds.size, 0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeEmptyDataSetFrontFail() {
    ConcurrentArrayList.makeEmptyDataSet(-1, 0);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void makeEmptyDataSetRearFail() {
    ConcurrentArrayList.makeEmptyDataSet(0, -1);
  }
}
