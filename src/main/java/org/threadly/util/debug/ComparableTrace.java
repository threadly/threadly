package org.threadly.util.debug;

import java.util.Arrays;

/**
 * Class which represents a stack trace.  The is used so we can track how many times a given 
 * stack is seen.
 * 
 * @since 5.20
 */
class ComparableTrace implements Comparable<ComparableTrace> {
  protected final StackTraceElement[] elements;
  protected final int hash;
  
  public ComparableTrace(StackTraceElement[] elements) {
    this.elements = elements;
    
    int h = 0;
    for (StackTraceElement e: elements) {
      h ^= e.hashCode();
    }
    hash = h;
  }
  
  @Override
  public int hashCode() {
    return hash;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else {
      try {
        ComparableTrace t = (ComparableTrace) o;
        if (t.hash != hash) {
          return false;
        } else {
          return Arrays.equals(t.elements, elements);
        }
      } catch (ClassCastException e) {
        return false;
      }
    }
  }
  
  @Override
  public int compareTo(ComparableTrace t) {
    return this.hash - t.hash;
  }
}