package org.threadly.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ArrayIteratorTest {
  @Test
  public void makeIteratorNullTest() {
    assertFalse(ArrayIterator.makeIterator(null).hasNext());
  }
  
  @Test
  public void makeIteratorEmptyArrayTest() {
    assertFalse(ArrayIterator.makeIterator(new String[0]).hasNext());
  }
  
  @Test
  public void makeIteratorTest() {
    String[] testArray = 
        new String[] { StringUtils.makeRandomString(8), StringUtils.makeRandomString(8), 
                       StringUtils.makeRandomString(8), StringUtils.makeRandomString(8) };
    Iterator<String> expectIt = Arrays.asList(testArray).iterator();
    Iterator<String> testIt = ArrayIterator.makeIterator(testArray);
    
    verifyIterator(expectIt, testIt);
  }
  
  private static void verifyIterator(Iterator<String> expectIt, Iterator<String> testIt) {
    while (expectIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(expectIt.next(), testIt.next());
    }
    assertFalse(testIt.hasNext());
  }
  
  @Test
  public void makeIterableTest() {
    String[] testArray = 
        new String[] { StringUtils.makeRandomString(8), StringUtils.makeRandomString(8), 
                       StringUtils.makeRandomString(8), StringUtils.makeRandomString(8) };
    Iterator<String> expectIt = Arrays.asList(testArray).iterator();
    Iterator<String> testIt = ArrayIterator.makeIterable(testArray).iterator();

    verifyIterator(expectIt, testIt);
  }
  
  @Test
  public void makeIterableReuseTest() {
    String[] testArray = new String[] { StringUtils.makeRandomString(8) };
    Iterable<String> iterable = ArrayIterator.makeIterable(testArray, false);
    
    assertTrue(iterable.iterator() == iterable.iterator());
    
    Iterator<String> testIt = iterable.iterator();
    while (testIt.hasNext()) {
      testIt.next();
    }
    
    iterable.iterator();  // should reset
    assertTrue(testIt.hasNext());
  }
}
