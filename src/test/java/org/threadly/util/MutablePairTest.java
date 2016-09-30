package org.threadly.util;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class MutablePairTest extends PairTest {
  @Override
  protected <T> MutablePair<T, T> makePair(T left, T right) {
    return new MutablePair<>(left, right);
  }
  
  @Test
  public void setAndGetLeftTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = new MutablePair<>();
    p.setLeft(strValue);
    assertEquals(strValue, p.getLeft());
  }
  
  @Test
  public void setAndGetRightTest() {
    String strValue = StringUtils.makeRandomString(5);
    MutablePair<String, String> p = new MutablePair<>();
    p.setRight(strValue);
    assertEquals(strValue, p.getRight());
  }
  
}
