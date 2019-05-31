package org.threadly.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implementation of {@link Iterator} which will go over an Object array.  Like most Iterators this 
 * class is NOT thread safe.
 *
 * @since 5.38
 * @param <T> The type of object to iterate over
 */
public class ArrayIterator<T> implements Iterator<T> {
  /**
   * Construct a new {@link Iterator} that will start at position zero of the provided array.
   * 
   * @param array Array to iterate over
   * @return An {@link Iterator} that will go over the provided array
   */
  public static <T> Iterator<T> makeIterator(T[] array) {
    if (array == null || array.length == 0) {
      return Collections.emptyIterator();
    }
    return new ArrayIterator<>(array);
  }
  
  /**
   * Use as a convience when {@link Iterable} is the desired interface.  This is equivalent to 
   * calling {@link #makeIterable(Object[], boolean)} with a {@code true} to specify new instances 
   * on iteration.
   *  
   * @param array Array to be iterated over
   * @return An {@link Iterable} that will go over the array
   */
  public static <T> Iterable<T> makeIterable(T[] array) {
    return makeIterable(array, true);
  }

  /**
   * Use as a convience when {@link Iterable} is the desired interface.  If the returned 
   * {@link Iterable} wont be invoked concurrently, then this can be called to reuse the 
   * {@link Iterator} instance by simply passing {@code false} for {@code newInstanceOnCall}.  If 
   * {@code true} is specified then a new instance will be returned on every invocation, allowing 
   * for the iterator to be created concurrently and not have the state of each iterator be impacted.
   * If {@code false} is provided then the returned {@link Iterator} will be reset each time.
   *  
   * @param array Array to be iterated over
   * @param newInstanceOnCall {@code true} to create new instances on each invocation of {@link Iterable#iterator()}
   * @return An {@link Iterable} that will go over the array
   */
  public static <T> Iterable<T> makeIterable(T[] array, boolean newInstanceOnCall) {
    if (newInstanceOnCall) {
      return () -> new ArrayIterator<>(array);
    } else {
      ArrayIterator<T> it = new ArrayIterator<>(array);
      return () -> { it.reset(); return it; };
    }
  }
  
  protected final T[] array;
  private int pos = 0;
  
  /**
   * Construct a new {@link Iterator} that will start at position zero of the provided array.
   * 
   * @param array Array to iterate over
   */
  protected ArrayIterator(T[] array) {
    this.array = array;
  }

  @Override
  public boolean hasNext() {
    return pos < array.length;
  }

  @Override
  public T next() {
    if (! hasNext()) {
      throw new NoSuchElementException();
    }
    return array[pos++];
  }
  
  /**
   * Reset the iterator back to position {@code 0}.
   */
  public void reset() {
    pos = 0;
  }
}
