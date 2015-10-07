package org.threadly.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <p>A simple tuple implementation (every library needs one, right?).  This is designed to be a 
 * minimal and light weight pair holder.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class Pair<L, R> {
  private static final short LEFT_PRIME = 13;
  private static final short RIGHT_PRIME = 31;
  
  /**
   * Collect all the non-null left references into a new List.  A simple implementation which 
   * iterates over a source collection and collects all non-null left references into a new list 
   * that can be manipulated or referenced.
   *  
   * @param source Source collection of pairs
   * @return New list that contains non-null left references
   */
  public static <T> List<T> collectLeft(Collection<? extends Pair<T, ?>> source) {
    List<T> result = new ArrayList<T>(source.size());
    for (Pair<T, ?> p : source) {
      if (p.left != null) {
        result.add(p.left);
      }
    }
    return result;
  }

  /**
   * Collect all the non-null right references into a new List.  A simple implementation which 
   * iterates over a source collection and collects all non-null right references into a new list 
   * that can be manipulated or referenced.
   *  
   * @param source Source collection of pairs
   * @return New list that contains non-null right references
   */
  public static <T> List<T> collectRight(Collection<? extends Pair<?, T>> source) {
    List<T> result = new ArrayList<T>(source.size());
    for (Pair<?, T> p : source) {
      if (p.right != null) {
        result.add(p.right);
      }
    }
    return result;
  }
  
  /**
   * Simple search to see if a collection of pairs contains a given left value.  It is assumed 
   * that the iterator will not return any {@code null} elements.
   * 
   * @param search Iterable to search over
   * @param value Value to be searching for from left elements
   * @return {@code true} if the value is found as a left element from the iterable provided
   */
  public static boolean containsLeft(Iterable<? extends Pair<?, ?>> search, Object value) {
    for (Pair<?, ?> p : search) {
      if (p.left == null) {
        if (value == null) {
          return true;
        }
      } else if (p.left.equals(value)) {
        return true;
      }
    }
    
    return false;
  }

  /**
   * Simple search to see if a collection of pairs contains a given right value.  It is assumed 
   * that the iterator will not return any {@code null} elements.
   * 
   * @param search Iterable to search over
   * @param value Value to be searching for from right elements
   * @return {@code true} if the value is found as a right element from the iterable provided
   */
  public static boolean containsRight(Iterable<? extends Pair<?, ?>> search, Object value) {
    for (Pair<?, ?> p : search) {
      if (p.left == null) {
        if (value == null) {
          return true;
        }
      } else if (p.right.equals(value)) {
        return true;
      }
    }
    
    return false;
  }
  
  // not final so extending classes can mutate
  protected L left;
  protected R right;
  
  /**
   * Constructs a new pair, providing the left and right objects to be held.
   * 
   * @param left Left reference
   * @param right Right reference
   */
  public Pair(L left, R right) {
    this.left = left;
    this.right = right;
  }
  
  /**
   * Getter to get the left reference stored in the pair.
   * 
   * @return Left reference
   */
  public L getLeft() {
    return left;
  }

  /**
   * Getter to get the right reference stored in the pair.
   * 
   * @return Right reference
   */
  public R getRight() {
    return right;
  }
  
  @Override
  public String toString() {
    return Pair.class.getSimpleName() + '[' + left + ',' + right + ']';
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof Pair) {
      Pair<?, ?> p = (Pair<?, ?>)o;
      if (! (left == p.left || (left != null && left.equals(p.left)))) {
        return false;
      } else if (! (right == p.right || (right != null && right.equals(p.right)))) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    int leftHash = left == null ? LEFT_PRIME : left.hashCode();
    int rightHash = right == null ? RIGHT_PRIME : right.hashCode();
    return leftHash ^ rightHash;
  }
}
