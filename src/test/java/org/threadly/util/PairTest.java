package org.threadly.util;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class PairTest {
  protected <T> Pair<T, T> makePair(T left, T right) {
    return new Pair<T, T>(left, right);
  }
  
  @Test
  public void collectLeftTest() {
    List<String> expectedResult = new ArrayList<String>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = StringUtils.makeRandomString(10);
      expectedResult.add(str);
      pairs.add(makePair(str, StringUtils.makeRandomString(5)));
      pairs.add(makePair(null, StringUtils.makeRandomString(5)));
    }
    
    List<String> result = Pair.collectLeft(pairs);
    
    assertEquals(TEST_QTY, result.size());
    assertTrue(expectedResult.equals(result));
  }
  
  @Test
  public void collectRightTest() {
    List<String> expectedResult = new ArrayList<String>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = StringUtils.makeRandomString(10);
      expectedResult.add(str);
      pairs.add(makePair(StringUtils.makeRandomString(5), str));
      pairs.add(makePair(StringUtils.makeRandomString(5), null));
    }
    
    List<String> result = Pair.collectRight(pairs);
    
    assertEquals(TEST_QTY, result.size());
    assertTrue(expectedResult.equals(result));
  }
  
  @Test
  public void containsLeftTest() {
    containsLeftTest(StringUtils.makeRandomString(10));
  }
  
  @Test
  public void containsLeftNullTest() {
    containsLeftTest(null);
  }
  
  private void containsLeftTest(String searchStr) {
    String rightStr = StringUtils.makeRandomString(20);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY + 1);
    for (int i = 0; i < TEST_QTY; i++) {
      if (i == TEST_QTY / 2) {
        pairs.add(makePair(searchStr, rightStr));
      }
      pairs.add(makePair(StringUtils.makeRandomString(5), rightStr));
    }
    
    assertFalse(Pair.containsLeft(pairs, rightStr));
    assertTrue(Pair.containsLeft(pairs, searchStr));
  }
  
  @Test
  public void containsRightTest() {
    containsRightTest(StringUtils.makeRandomString(10));
  }
  
  @Test
  public void containsRightNullTest() {
    containsRightTest(StringUtils.makeRandomString(10));
  }
  
  private void containsRightTest(String searchStr) {
    String leftStr = StringUtils.makeRandomString(20);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY + 1);
    for (int i = 0; i < TEST_QTY; i++) {
      if (i == TEST_QTY / 2) {
        pairs.add(makePair(leftStr, searchStr));
      }
      pairs.add(makePair(leftStr, StringUtils.makeRandomString(5)));
    }
    
    assertFalse(Pair.containsRight(pairs, leftStr));
    assertTrue(Pair.containsRight(pairs, searchStr));
  }
  
  @Test
  public void getRightFromLeftTest() {
    getRightFromLeftTest(StringUtils.makeRandomString(10));
  }
  
  @Test
  public void getRightFromLeftNullTest() {
    getRightFromLeftTest(null);
  }
  
  private void getRightFromLeftTest(String searchStr) {
    String rightStr = StringUtils.makeRandomString(20);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY + 1);
    for (int i = 0; i < TEST_QTY; i++) {
      if (i == TEST_QTY / 2) {
        pairs.add(makePair(searchStr, rightStr));
      }
      pairs.add(makePair(StringUtils.makeRandomString(5), StringUtils.makeRandomString(5)));
    }
    
    assertTrue(Pair.getRightFromLeft(pairs, searchStr) == rightStr);
  }
  
  @Test
  public void getLeftFromRightTest() {
    getLeftFromRightTest(StringUtils.makeRandomString(10));
  }
  
  @Test
  public void getLeftFromRightNullTest() {
    getLeftFromRightTest(null);
  }
  
  private void getLeftFromRightTest(String searchStr) {
    String leftStr = StringUtils.makeRandomString(20);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY + 1);
    for (int i = 0; i < TEST_QTY; i++) {
      if (i == TEST_QTY / 2) {
        pairs.add(makePair(leftStr, searchStr));
      }
      pairs.add(makePair(StringUtils.makeRandomString(5), StringUtils.makeRandomString(5)));
    }
    
    assertTrue(Pair.getLeftFromRight(pairs, searchStr) == leftStr);
  }
  
  @Test
  public void constructAndGetNullTest() {
    Pair<String, String> p = makePair(null, null);
    assertNull(p.getLeft());
    assertNull(p.getRight());
  }
  
  @Test
  public void constructAndGetTest() {
    String leftStr = StringUtils.makeRandomString(10);
    String rightStr = StringUtils.makeRandomString(10);
    
    Pair<String, String> p =makePair(leftStr, rightStr);
    assertTrue(p.getLeft() == leftStr);
    assertTrue(p.getRight() == rightStr);
  }
  
  @Test
  public void toStringTest() {
    String leftStr = StringUtils.makeRandomString(10);
    String rightStr = StringUtils.makeRandomString(10);
    
    Pair<String, String> p = makePair(leftStr, rightStr);
    String result = p.toString();
    assertTrue(result.contains(leftStr));
    assertTrue(result.contains(rightStr));
    assertTrue(result.indexOf(leftStr) < result.indexOf(rightStr));
  }
  
  @Test
  public void equalsNullTest() {
    Pair<String, String> p1 = makePair(null, null);
    Pair<String, String> p2 = makePair(null, null);
    
    assertTrue(p1.equals(p1));
    assertTrue(p1.equals(p2));
  }
  
  @Test
  public void equalsTest() {
    String leftStr = StringUtils.makeRandomString(10);
    String rightStr = StringUtils.makeRandomString(10);
    
    Pair<String, String> p1 = makePair(leftStr, rightStr);
    Pair<String, String> p2 = makePair(leftStr, rightStr);
    
    assertTrue(p1.equals(p1));
    assertTrue(p1.equals(p2));
  }
  
  @Test
  public void equalsNullFailTest() {
    Pair<String, String> p1 = makePair(null, null);
    Pair<String, String> p2 = makePair(StringUtils.makeRandomString(10), null);
    Pair<String, String> p3 = makePair(null, StringUtils.makeRandomString(10));
    
    assertFalse(p1.equals(p2));
    assertFalse(p1.equals(p3));
    assertFalse(p2.equals(p3));
  }
  
  @Test
  public void equalsFailTest() {
    Pair<String, String> p1 = makePair(StringUtils.makeRandomString(5), 
                                       StringUtils.makeRandomString(5));
    Pair<String, String> p2 = makePair(StringUtils.makeRandomString(5), 
                                       StringUtils.makeRandomString(5));
    
    assertFalse(p1.equals(p2));
  }
  
  @Test
  public void hashCodeNullTest() {
    Pair<String, String> p1 = makePair(null, null);
    Pair<String, String> p2 = makePair(null, null);
    Pair<String, String> p3 = makePair(StringUtils.makeRandomString(10), null);
    Pair<String, String> p4 = makePair(null, StringUtils.makeRandomString(10));
    
    assertEquals(p1.hashCode(), p2.hashCode());
    assertTrue(p1.hashCode() != p3.hashCode());
    assertTrue(p1.hashCode() != p4.hashCode());
    assertTrue(p3.hashCode() != p4.hashCode());
  }
}
