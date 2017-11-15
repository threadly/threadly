package org.threadly.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A simple tuple implementation (every library needs one, right?).  This is designed to be a 
 * minimal and light weight pair holder.
 * 
 * @since 4.4.0
 * @param <L> Type of 'left' object to be held
 * @param <R> Type of 'right' object to be held
 */
public class Pair<L, R> {
  private static final short LEFT_PRIME = 13;
  private static final short RIGHT_PRIME = 31;
  
  /**
   * Function to assist in transforming the left side of pairs from one form to another.  This will 
   * iterate through the source, applying the provided transformer function to each left side entry.  
   * The resulting list will contain new pair instances with the transformed left entries and the 
   * original right side entries.
   * <p>
   * If both the left and right sides must be transformed please see 
   * {@link #transform(Collection, Function, Function)}.
   * 
   * @param <OL> Original left side type for input source
   * @param <NL> Transformed left side type for resulting list
   * @param <R> Type of object held as pair's right reference
   * @param source Source collection to be iterating over
   * @param transformer Function to apply to the left side entries
   * @return New list of transformed pairs
   */
  public static <OL, NL, R> List<Pair<NL, R>> transformLeft(Collection<? extends Pair<? extends OL, 
                                                                                      ? extends R>> source, 
                                                            Function<? super OL, ? extends NL> transformer) {
    return transform(source, transformer, (r) -> r);
  }

  /**
   * Function to assist in transforming the right side of pairs from one form to another.  This will 
   * iterate through the source, applying the provided transformer function to each right side entry.  
   * The resulting list will contain new pair instances with the transformed right entries and the 
   * original left side entries.
   * <p>
   * If both the left and right sides must be transformed please see 
   * {@link #transform(Collection, Function, Function)}.
   * 
   * @param <OR> Original right side type for input source
   * @param <NR> Transformed right side type for resulting list
   * @param <L> Type of object held as pair's left reference
   * @param source Source collection to be iterating over
   * @param transformer Function to apply to the right side entries
   * @return New list of transformed pairs
   */
  public static <L, OR, NR> List<Pair<L, NR>> transformRight(Collection<? extends Pair<? extends L, 
                                                                                       ? extends OR>> source, 
                                                             Function<? super OR, ? extends NR> transformer) {
    return transform(source, (l) -> l, transformer);
  }
  
  /**
   * Function to assist in transforming a collection of pairs into a new resulting list.  This 
   * simply iterates over the provided collection and applies the left and right transformers to 
   * each pair.  Returning a new list with the resulting transformed values contained in the pairs.
   * 
   * @param <OL> Original left side type for input source
   * @param <OR> Original right side type for input source
   * @param <NL> Transformed left side type for resulting list
   * @param <NR> Transformed right side type for resulting list
   * @param source Source collection to be iterating over
   * @param leftTransformer Function to apply to the left side entries
   * @param rightTransformer Function to apply to the right side entries
   * @return New list of transformed pairs
   */
  public static <OL, NL, OR, NR> List<Pair<NL, NR>> transform(Collection<? extends Pair<? extends OL, 
                                                                                        ? extends OR>> source, 
                                                              Function<? super OL, ? extends NL> leftTransformer,
                                                              Function<? super OR, ? extends NR> rightTransformer) {
    if (source.isEmpty()) {
      return Collections.emptyList();
    }
    List<Pair<NL, NR>> result = new ArrayList<>(source.size());
    for (Pair<? extends OL, ? extends OR> p : source) {
      result.add(new Pair<>(leftTransformer.apply(p.left), 
                            rightTransformer.apply(p.right)));
    }
    return result;
  }
  
  /**
   * Goes through source {@link Iterable} and provides all left entries to a given consumer.
   * 
   * @param <L> Type of object held as pair's left reference
   * @param source Source to iterate through
   * @param consumer Consumer to provide left entries to
   */
  public static <L> void applyToLeft(Iterable<? extends Pair<? extends L, ?>> source, 
                                     Consumer<? super L> consumer) {
    for (Pair<? extends L, ?> p : source) {
      consumer.accept(p.left);
    }
  }

  /**
   * Goes through source {@link Iterable} and provides all right entries to a given consumer.
   * 
   * @param <R> Type of object held as pair's right reference
   * @param source Source to iterate through
   * @param consumer Consumer to provide right entries to
   */
  public static <R> void applyToRight(Iterable<? extends Pair<?, ? extends R>> source, 
                                      Consumer<? super R> consumer) {
    for (Pair<?, ? extends R> p : source) {
      consumer.accept(p.right);
    }
  }

  /**
   * Convert a map into a list of Pair's where the left side of the pair contains the value and 
   * the right side is the corresponding value.
   * 
   * @param <L> Type of object held as pair's left reference
   * @param <R> Type of object held as pair's right reference
   * @param map Map to source entries from
   * @return A list of pairs sourced from the map's entries
   */
  public static <L, R> List<Pair<L, R>> convertMap(Map<? extends L, ? extends R> map) {
    if (map.isEmpty()) {
      return Collections.emptyList();
    }
    List<Pair<L, R>> result = new ArrayList<>(map.size());
    for (Map.Entry<? extends L, ? extends R> e : map.entrySet()) {
      result.add(new Pair<>(e.getKey(), e.getValue()));
    }
    return result;
  }
  
  /**
   * Split a collection of pair's into a pair of two collections.  This is more efficient than 
   * invoking {@link #getLeft()} and {@link #getRight()} separately.  Similar to those functions, 
   * this will only collect non-null entries.  If there is a {@code null} left or right entry then 
   * the indexes will no longer match each other between the two lists.
   * 
   * @param <L> Type of object held as pair's left reference
   * @param <R> Type of object held as pair's right reference
   * @param source Source collection of pairs
   * @return Pair of two collections with the left and right halves
   */
  public static <L, R> Pair<List<L>, List<R>> split(Collection<? extends Pair<? extends L, ? extends R>> source) {
    int estimatedCount = source.size();
    List<L> left = new ArrayList<>(estimatedCount);
    List<R> right = new ArrayList<>(estimatedCount);
    for (Pair<? extends L, ? extends R> p : source) {
      if (p.left != null) {
        left.add(p.left);
      }
      if (p.right != null) {
        right.add(p.right);
      }
    }
    return new Pair<>(left, right);
  }
  
  /**
   * Convert a {@link Pair} {@link Iterator} into an iterator that returns the left items.  This 
   * has the minor advantage over {@link #collectLeft(Collection)} in that the collection is not 
   * iterated / copied.  Allowing for potential concurrent structures to provide their special 
   * iterator behavior through this, as well as avoiding a potential short term memory copy.
   * 
   * @param <T> Type of object held as pair's left reference
   * @param i Iterable to source pairs from
   * @return An iterator that extracts out the left entry of each pair
   */
  public static <T> Iterable<T> iterateLeft(Iterable<? extends Pair<? extends T, ?>> i) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return iterateLeft(i.iterator());
      }
    };
  }
  
  /**
   * Convert a {@link Pair} {@link Iterator} into an iterator that returns the left items.  This 
   * has the minor advantage over {@link #collectLeft(Collection)} in that the collection is not 
   * iterated / copied.  Allowing for potential concurrent structures to provide their special 
   * iterator behavior through this, as well as avoiding a potential short term memory copy.
   * 
   * @param <T> Type of object held as pair's left reference
   * @param i Iterator to source pairs from
   * @return An iterator that extracts out the left entry of each pair
   */
  public static <T> Iterator<T> iterateLeft(Iterator<? extends Pair<? extends T, ?>> i) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public T next() {
        return i.next().left;
      }
    };
  }

  /**
   * Convert a {@link Pair} {@link Iterator} into an iterator that returns the right items.  This 
   * has the minor advantage over {@link #collectRight(Collection)} in that the collection is not 
   * iterated / copied.  Allowing for potential concurrent structures to provide their special 
   * iterator behavior through this, as well as avoiding a potential short term memory copy.
   * 
   * @param <T> Type of object held as pair's right reference
   * @param i Iterable to source pairs from
   * @return An iterator that extracts out the right entry of each pair
   */
  public static <T> Iterable<T> iterateRight(Iterable<? extends Pair<?, ? extends T>> i) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return iterateRight(i.iterator());
      }
    };
  }

  /**
   * Convert a {@link Pair} {@link Iterator} into an iterator that returns the right items.  This 
   * has the minor advantage over {@link #collectRight(Collection)} in that the collection is not 
   * iterated / copied.  Allowing for potential concurrent structures to provide their special 
   * iterator behavior through this, as well as avoiding a potential short term memory copy.
   * 
   * @param <T> Type of object held as pair's right reference
   * @param i Iterator to source pairs from
   * @return An iterator that extracts out the right entry of each pair
   */
  public static <T> Iterator<T> iterateRight(Iterator<? extends Pair<?, ? extends T>> i) {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public T next() {
        return i.next().right;
      }
    };
  }
  
  /**
   * Collect all the non-null left references into a new List.  A simple implementation which 
   * iterates over a source collection and collects all non-null left references into a new list 
   * that can be manipulated or referenced.
   *  
   * @param <T> Type of object held as pair's left reference
   * @param source Source collection of pairs
   * @return New list that contains non-null left references
   */
  public static <T> List<T> collectLeft(Collection<? extends Pair<? extends T, ?>> source) {
    if (source.isEmpty()) {
      return Collections.emptyList();
    }
    List<T> result = new ArrayList<>(source.size());
    for (Pair<? extends T, ?> p : source) {
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
   * @param <T> Type of object held as pair's right reference
   * @param source Source collection of pairs
   * @return New list that contains non-null right references
   */
  public static <T> List<T> collectRight(Collection<? extends Pair<?, ? extends T>> source) {
    if (source.isEmpty()) {
      return Collections.emptyList();
    }
    List<T> result = new ArrayList<>(source.size());
    for (Pair<?, ? extends T> p : source) {
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
      if (p.right == null) {
        if (value == null) {
          return true;
        }
      } else if (p.right.equals(value)) {
        return true;
      }
    }
    
    return false;
  }
  
  /**
   * Get the right side of a pair by searching for a matching left side.  This iterates over the 
   * provided source, and once the first pair with a left that matches (via 
   * {@link Object#equals(Object)}), the right side is returned.  If no match is found, 
   * {@code null} will be returned.  Although the implementer must be aware that since nulls can 
   * be kept kept inside pairs, that does not strictly indicate a match failure.
   * 
   * @param <T> Type of object held as pair's right reference
   * @param search Iteratable to search through looking for a match
   * @param left Object to be looking searching for as a left reference
   * @return Corresponding right reference or {@code null} if none is found
   */
  public static <T> T getRightFromLeft(Iterable<? extends Pair<?, ? extends T>> search, Object left) {
    for (Pair<?, ? extends T> p : search) {
      if (p.left == null) {
        if (left == null) {
          return p.right;
        }
      } else if (p.left.equals(left)) {
        return p.right;
      }
    }
    
    return null;
  }
  
  /**
   * Get the left side of a pair by searching for a matching right side.  This iterates over the 
   * provided source, and once the first pair with a right that matches (via 
   * {@link Object#equals(Object)}), the left side is returned.  If no match is found, 
   * {@code null} will be returned.  Although the implementer must be aware that since nulls can 
   * be kept kept inside pairs, that does not strictly indicate a match failure.
   * 
   * @param <T> Type of object held as pair's left reference
   * @param search Iteratable to search through looking for a match
   * @param right Object to be looking searching for as a left reference
   * @return Corresponding left reference or {@code null} if none is found
   */
  public static <T> T getLeftFromRight(Iterable<? extends Pair<? extends T, ?>> search, Object right) {
    for (Pair<? extends T, ?> p : search) {
      if (p.right == null) {
        if (right == null) {
          return p.left;
        }
      } else if (p.right.equals(right)) {
        return p.left;
      }
    }
    
    return null;
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
