package org.threadly.concurrent.collections;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

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
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class ConcurrentArrayListTest {
  private ConcurrentArrayList<String> testList;
  
  @Before
  public void setup() {
    testList = new ConcurrentArrayList<String>();
  }
  
  @After
  public void tearDown() {
    testList = null;
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new ConcurrentArrayList<String>(null, new Object());
    fail("Exception should have thrown");
  }
  
  @Test
  public void toStringTest() {
    testList.add("foo");
    String testStr = testList.toString();
    
    assertNotNull(testStr);
    assertTrue(testStr.length() > 2);
  }
  
  @Test
  public void getModificationLockTest() {
    Object testLock = new Object();
    ConcurrentArrayList<String> testList = new ConcurrentArrayList<String>(testLock);
    
    assertTrue(testLock == testList.getModificationLock());
  }
  
  @Test
  public void setFrontPaddingTest() {
    testList.setFrontPadding(1);
    assertEquals(1, testList.getFrontPadding());
    
    // make some modifications
    testList.add("foo");
    testList.offer("bar");
    testList.remove(0);
    
    assertEquals(1, testList.getFrontPadding());
  }

  @Test (expected = IllegalArgumentException.class)
  public void setFrontPaddingFail() {
    testList.setFrontPadding(-1);
  }
  
  @Test
  public void setRearPaddingTest() {
    testList.setRearPadding(1);
    assertEquals(1, testList.getRearPadding());
    
    // make some modifications
    testList.add("foo");
    testList.offer("bar");
    testList.remove(0);
    
    assertEquals(1, testList.getRearPadding());
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
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void getInvalidIndexTest() {
    testList.get(1);
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
  public void pushTest() {
    addFirstOrPushTest(false);
  }
  
  @Test
  public void addFirstTest() {
    addFirstOrPushTest(true);
  }
  
  public void addFirstOrPushTest(boolean addFirst) {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      if (addFirst) {
        testList.addFirst(str);
      } else {
        testList.push(str);
      }
      assertEquals(str, testList.getFirst());
    }
  }
  
  @Test
  public void addLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(str, testList.getLast());
    }
  }
  
  @Test
  public void offerFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.offerFirst(str));
      assertEquals(str, testList.getFirst());
    }
  }
  
  @Test
  public void offerLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      assertTrue(testList.offerLast(str));
      assertEquals(str, testList.getLast());
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
  public void addNullTest() {
    ListTests.addNullTest(testList);
  }
  
  @Test
  public void addAllTest() {
    ListTests.addAllTest(testList);
  }
  
  @Test
  public void addIndexTest() {
    ListTests.addIndexTest(testList);
  }
  
  @Test
  public void addIndexFail() {
    ListTests.addIndexFail(testList);
  }
  
  @Test
  public void addAllIndexTest() {
    ListTests.addAllIndexTest(testList);
  }
  
  @Test
  public void addAllIndexFail() {
    ListTests.addAllIndexFail(testList);
  }
  
  @Test
  public void peekFirstTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addFirst(str);
      assertEquals(str, testList.peekFirst());
    }
  }
  
  @Test
  public void peekLastTest() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.addLast(str);
      assertEquals(str, testList.peekLast());
    }
  }
  
  @Test
  public void removeAllTest() {
    ListTests.removeAllTest(testList);
  }
  
  @Test
  public void removeFirstOccurrenceNotFoundTest() {
    assertFalse(testList.removeFirstOccurrence(null));
    assertFalse(testList.removeFirstOccurrence(new Object()));
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
      assertTrue(testList.removeFirstOccurrence(str));
    }
    
    assertEquals(secondStr.size(), testList.size());
    
    Iterator<String> it = secondStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  @Test
  public void removeLastOccurrenceNotFoundTest() {
    assertFalse(testList.removeLastOccurrence(null));
    assertFalse(testList.removeLastOccurrence(new Object()));
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
      assertTrue(testList.removeLastOccurrence(str));
    }
    
    assertEquals(firstStr.size(), testList.size());
    
    Iterator<String> it = firstStr.iterator();
    Iterator<String> testIt = testList.iterator();
    while (it.hasNext()) {
      assertTrue(testIt.next() == it.next());
    }
  }
  
  @Test
  public void removeTest() {
    removeOrPopTest(0);
  }
  
  @Test
  public void removeFirstTest() {
    removeOrPopTest(1);
  }
  
  @Test
  public void popTest() {
    removeOrPopTest(2);
  }
  
  public void removeOrPopTest(int removeType) {
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
      switch (removeType) {
        case 0:
          assertTrue(testList.remove() == next);
          break;
        case 1:
          assertTrue(testList.removeFirst() == next);
          break;
        case 2:
          assertTrue(testList.pop() == next);
          break;
        default:
          throw new UnsupportedOperationException("Unknown remove type: " + removeType);
      }
      removed++;
      assertEquals(TEST_QTY - removed, testList.size());
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
      assertEquals(TEST_QTY - removed, testList.size());
      assertFalse(testList.peekLast() == next);
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void removeLastFail() {
    testList.removeLast();
  }
  
  @Test
  public void pollTest() {
    pollTest(false);
  }
  
  @Test
  public void pollFirstTest() {
    pollTest(true);
  }
  
  public void pollTest(boolean pollFirst) {
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
      if (pollFirst) {
        assertTrue(testList.pollFirst() == next);
      } else {
        assertTrue(testList.poll() == next);
      }
      removed++;
      assertEquals(TEST_QTY - removed, testList.size());
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
      assertEquals(TEST_QTY - removed, testList.size());
      assertFalse(testList.peekLast() == next);
    }
  }
  
  @Test
  public void elementTest() {
    String foo = StringUtils.randomString(5);
    testList.add(foo);
    assertEquals(foo, testList.element());
    
    testList.add(StringUtils.randomString(5));
    assertEquals(foo, testList.element());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void elementFail() {
    testList.element();
    
    fail("Exception should have been thrown");
  }
  
  @Test
  public void removeMissingObjectTest() {
    ListTests.removeMissingObjectTest(testList);
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
  public void removeIndexFail() {
    ListTests.removeIndexFail(testList);
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

  
  @Test
  public void repositionObjectIndexFail() {
    try {
      testList.reposition(Integer.toString(0), 1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.reposition(Integer.toString(0), -1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  @Test (expected = NoSuchElementException.class)
  public void repositionObjectNotFoundFail() {
    for (int i = 0; i < TEST_QTY; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
    testList.reposition("foo", 0);
    fail("Exception should have been thrown");
  }
  
  @Test
  public void repositionIndexTest() {
    int testQty = Math.max(10, TEST_QTY);
    // should be a no-op
    testList.reposition(0, 0);
    
    for (int i = 0; i < testQty; i++) {
      String str = Integer.toString(i);
      testList.add(str);
    }
    
    testList.reposition(testQty - 1, 0);
    assertEquals(Integer.toString(testQty - 1), testList.get(0));
    assertEquals(Integer.toString(0), testList.get(1));

    String expectedNext = testList.get(6);
    testList.reposition(0, 5);
    assertEquals(Integer.toString(testQty - 1), // one less than index position because shifted right 
                 testList.get(4));
    assertEquals(expectedNext, testList.get(6));
    
    testList.reposition(1, 3);  // swap 1 to 2
    assertEquals(Integer.toString(2), testList.get(1));
    assertEquals(Integer.toString(1), testList.get(2));
  }
  
  @Test
  public void repositionIndexFail() {
    try {
      testList.reposition(-1, 0);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.reposition(0, -1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.reposition(0, 1);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
    try {
      testList.reposition(1, 0);
      fail("Exception should have been thrown");
    } catch (IndexOutOfBoundsException e) {
      // expected
    }
  }
  
  @Test
  public void setFail() {
    ListTests.setFail(testList);
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
    
    assertEquals(0, ds.size);
    
    ds = ConcurrentArrayList.makeEmptyDataSet(10, 10);
    assertEquals(0, ds.size);
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
