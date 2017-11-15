package org.threadly.util;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class PairTest {
  protected <T> Pair<T, T> makePair(T left, T right) {
    return new Pair<>(left, right);
  }
  
  protected List<Pair<String, String>> makeRandomPairs() {
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      pairs.add(makePair(StringUtils.makeRandomString(8), StringUtils.makeRandomString(8)));
    }
    return pairs;
  }
  
  @Test
  public void transformLeftTest() {
    final String transformedString = "foo";
    List<Pair<String, String>> pairs = makeRandomPairs();
    
    for (Pair<String, String> p : Pair.transformLeft(pairs, (s) -> transformedString)) {
      assertTrue(p.getLeft().equals(transformedString));
      assertFalse(p.getRight().equals(transformedString));
    }
  }
  
  @Test
  public void transformRightTest() {
    final String transformedString = "foo";
    List<Pair<String, String>> pairs = makeRandomPairs();
    
    for (Pair<String, String> p : Pair.transformRight(pairs, (s) -> transformedString)) {
      assertTrue(p.getRight().equals(transformedString));
      assertFalse(p.getLeft().equals(transformedString));
    }
  }
  
  @Test
  public void transformTest() {
    final String transformedString = "foo";
    List<Pair<String, String>> pairs = makeRandomPairs();
    
    for (Pair<String, String> p : Pair.transform(pairs, (s) -> transformedString, (s) -> transformedString)) {
      assertTrue(p.getRight().equals(transformedString));
      assertTrue(p.getLeft().equals(transformedString));
    }
  }
  
  @Test
  public void applyToLeftTest() {
    List<Pair<String, String>> pairs = makeRandomPairs();
    List<String> leftCollected = new ArrayList<>(pairs.size());
    
    Pair.applyToLeft(pairs, (s) -> leftCollected.add(s));
    
    assertEquals(Pair.collectLeft(pairs), leftCollected);
  }
  
  @Test
  public void applyToRightTest() {
    List<Pair<String, String>> pairs = makeRandomPairs();
    List<String> rightCollected = new ArrayList<>(pairs.size());
    
    Pair.applyToRight(pairs, (s) -> rightCollected.add(s));
    
    assertEquals(Pair.collectRight(pairs), rightCollected);
  }
  
  @Test
  public void convertMapTest() {
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < TEST_QTY; i++) {
      values.put(StringUtils.makeRandomString(8), StringUtils.makeRandomString(8));
    }
    
    List<Pair<String, String>> pairs = Pair.convertMap(values);
    
    assertEquals(values.size(), pairs.size());
    List<String> leftSide = Pair.collectLeft(pairs);
    List<String> rightSide = Pair.collectRight(pairs);
    for (Map.Entry<String, String> e : values.entrySet()) {
      assertTrue(leftSide.contains(e.getKey()));
      assertTrue(rightSide.contains(e.getValue()));
    }
  }
  
  @Test
  public void splitTest() {
    List<String> expectedLeftResult = new ArrayList<String>(TEST_QTY);
    List<String> expectedRightResult = new ArrayList<String>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>(TEST_QTY * 2);
    for (int i = 0; i < TEST_QTY; i++) {
      String leftStr = StringUtils.makeRandomString(10);
      String rightStr = StringUtils.makeRandomString(5);
      expectedLeftResult.add(leftStr);
      expectedRightResult.add(rightStr);
      pairs.add(makePair(leftStr, null));
      pairs.add(makePair(null, rightStr));
    }
    
    Pair<List<String>, List<String>> result = Pair.split(pairs);
    
    assertEquals(TEST_QTY, result.getLeft().size());
    assertEquals(TEST_QTY, result.getRight().size());
    assertTrue(expectedLeftResult.equals(result.getLeft()));
    assertTrue(expectedRightResult.equals(result.getRight()));
  }
  
  @Test
  public void iterateLeftTest() {
    List<String> expectedResult = new ArrayList<>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = StringUtils.makeRandomString(10);
      expectedResult.add(str);
      pairs.add(makePair(str, StringUtils.makeRandomString(5)));
    }
    
    Iterator<String> testIt = Pair.iterateLeft(pairs).iterator();
    Iterator<String> verifyIt = expectedResult.iterator();
    while (verifyIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(verifyIt.next(), testIt.next());
    }
    assertFalse(testIt.hasNext());
  }
  
  @Test
  public void iterateRightTest() {
    List<String> expectedResult = new ArrayList<>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      String str = StringUtils.makeRandomString(10);
      expectedResult.add(str);
      pairs.add(makePair(StringUtils.makeRandomString(5), str));
    }
    
    Iterator<String> testIt = Pair.iterateRight(pairs).iterator();
    Iterator<String> verifyIt = expectedResult.iterator();
    while (verifyIt.hasNext()) {
      assertTrue(testIt.hasNext());
      assertEquals(verifyIt.next(), testIt.next());
    }
    assertFalse(testIt.hasNext());
  }
  
  @Test
  public void collectLeftTest() {
    List<String> expectedResult = new ArrayList<>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY * 2);
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
    List<String> expectedResult = new ArrayList<>(TEST_QTY);
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY * 2);
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
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY + 1);
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
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY + 1);
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
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY + 1);
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
    List<Pair<String, String>> pairs = new ArrayList<>(TEST_QTY + 1);
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
