package org.threadly.util;

import java.util.List;
import java.util.function.Function;

/**
 * <p>A collection of utilities for searching and sorting against collections and other data 
 * providers.</p>
 * 
 * @since 5.0.0
 */
public class SortUtils {
  protected static final short MAX_STEPS_TILL_B_SEARCH_SWITCH = 5;
  
  /**
   * This function uses the binary search and adds a small amount of logic such that it determines 
   * the placement index for a given item.  It is designed to always return the index after any 
   * values which equal the provided insertion value.
   * 
   * @param values List of values to search over and find desired value
   * @param insertionValue value in relation to functions provided values to search for insertion point
   * @param randomAccessList boolean for optimization with binary search
   * @return the index to insert the key into the list
   */
  public static int getInsertionEndIndex(List<Long> values, long insertionValue, 
                                         boolean randomAccessList) {
    return getInsertionEndIndex(values::get, values.size() - 1, insertionValue, randomAccessList);
  }
  
  /**
   * This function uses the binary search and adds a small amount of logic such that it determines 
   * the placement index for a given value.  It is designed to always return the index after any 
   * values which equal the provided insertion value.
   * 
   * @param valueProvider Function which will provide values for requested indexes
   * @param absoluteMax maximum index (inclusive) to search within
   * @param insertionValue value in relation to functions provided values to search for insertion point
   * @param randomAccessList boolean for optimization with binary search
   * @return the index to insert the key into the list
   */
  public static int getInsertionEndIndex(Function<Integer, Long> valueProvider, int absoluteMax,
                                         long insertionValue, boolean randomAccessList) {
    int searchResult = binarySearch(valueProvider, absoluteMax, insertionValue, randomAccessList);
    if (searchResult >= 0) {
      while (searchResult <= absoluteMax && valueProvider.apply(searchResult) == insertionValue) {
        searchResult++;
      }
      return searchResult;
    } else {
      return Math.abs(searchResult) - 1;
    }
  }
  
  /**
   * A faster binary search algorithm for sorting a list.  This algorithm works by actually 
   * knowing the values and making smart decisions about how far to jump in the list based on 
   * those values.  Which is why this can not take in a comparable interface like Collections 
   * does.  This was adapted from code posted from this blog post: http://ochafik.com/blog/?p=106
   * 
   * @param values List of values to search over and find desired value
   * @param insertionValue value in relation to functions provided values to search for insertion point
   * @param randomAccessList {@code true} to optimize for list that have cheap random access
   * @return index where found, or -(insertion point) - 1 if not found
   */
  public static int binarySearch(List<Long> values, long insertionValue, boolean randomAccessList) {
    return binarySearch(values::get, values.size() - 1, insertionValue, randomAccessList);
  }
  
  /**
   * A faster binary search algorithm for sorting a list.  This algorithm works by actually 
   * knowing the values and making smart decisions about how far to jump in the list based on 
   * those values.  Which is why this can not take in a comparable interface like Collections 
   * does.  This was adapted from code posted from this blog post: http://ochafik.com/blog/?p=106
   * 
   * @param valueProvider Function which will provide values for requested indexes
   * @param absoluteMax maximum index (inclusive) to search within
   * @param insertionValue value in relation to functions provided values to search for insertion point
   * @param randomAccessList {@code true} to optimize for list that have cheap random access
   * @return index where found, or -(insertion point) - 1 if not found
   */
  public static int binarySearch(Function<Integer, Long> valueProvider, int absoluteMax,
                                 long insertionValue, boolean randomAccessList) {
    if (absoluteMax < 0) {
      return -1;
    }
    
    int min = 0;
    int max = absoluteMax;
    long minVal = valueProvider.apply(min);
    long maxVal = valueProvider.apply(max);
    
    short nPreviousSteps = 1;
    while (true) {
      if (insertionValue <= minVal) {
        return insertionValue == minVal ? min : -1 - min;
      } else if (insertionValue >= maxVal) {
        return insertionValue == maxVal ? max : -2 - max;
      }
      
      int pivot;
      // A typical binarySearch algorithm uses pivot = (min + max) / 2.
      // The pivot we use here tries to be smarter and to choose a pivot 
      // close to the expected location of the key. This reduces dramatically 
      // the number of steps needed to get to the key.  However, it does not 
      // work well with a logarithmic distribution of values. When the key is 
      // not found quickly the smart way, we switch to the standard pivot.
      if (nPreviousSteps > MAX_STEPS_TILL_B_SEARCH_SWITCH) {
        pivot = (min + max) >> 1;
        // stop increasing nPreviousSteps from now on
      } else {
        // We cannot do the following operations in int precision, because there might be overflows.
        // using a float is better performing than using a long (even on 64bit)
        pivot = min + (int)((insertionValue - (float)minVal) / (maxVal - (float)minVal) * (max - min));
        nPreviousSteps++;
      }
      
      long pivotVal = valueProvider.apply(pivot);
      
      if (insertionValue > pivotVal) {
        min = pivot + 1;
        if (min > absoluteMax) {
          return absoluteMax + 1;
        }
        minVal = valueProvider.apply(min);
        if (randomAccessList) {
          // if cheap to check, we should see what the value is at this point
          max--;
          maxVal = valueProvider.apply(max);
        }
      } else if (insertionValue < pivotVal) {
        max = pivot - 1;
        if (max < 0) {
          return 0;
        }
        maxVal = valueProvider.apply(max);
        if (randomAccessList) {
          // if cheap to check, we should see what the value is at this point
          min++;
          minVal = valueProvider.apply(min);
        }
      } else {
        return pivot;
      }
    }
  }
}
