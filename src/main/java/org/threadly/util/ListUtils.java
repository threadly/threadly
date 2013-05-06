package org.threadly.util;

import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * @author jent - Mike Jensen
 */
public class ListUtils {
  /**
   * This function uses the binary search and adds a small amount of logic
   * such that it determines the placement index for a given item.  It is 
   * designed to always place the item after any existing items that match the 
   * key's delay.
   * 
   * @param list List to search entries for placement
   * @param key key for searching placement of
   * @return the index to insert the key into the list
   */
  public static int getInsertionEndIndex(List<? extends Delayed> list, 
                                         Delayed key) {
    return getInsertionEndIndex(list, key, list instanceof RandomAccess);
  }
  
  /**
   * This function uses the binary search and adds a small amount of logic
   * such that it determines the placement index for a given item.  It is 
   * designed to always place the item after any existing items that match the 
   * key's delay.
   * 
   * @param list List to search entries for placement
   * @param key key for searching placement of
   * @param randomAccessList boolean for optimization with binary search
   * @return the index to insert the key into the list
   */
  public static int getInsertionEndIndex(List<? extends Delayed> list, 
                                         Delayed key, boolean randomAccessList) {
    int searchResult = binarySearch(list, key, 
                                    randomAccessList);
    if (searchResult >= 0) {
      long keyDelay = key.getDelay(TimeUnit.MILLISECONDS);
      Iterator<? extends Delayed> it = list.listIterator(searchResult);
      while (it.hasNext() && it.next().getDelay(TimeUnit.MILLISECONDS) == keyDelay) {
        searchResult++;
      }
      return searchResult;
    } else {
      return Math.abs(searchResult) - 1;
    }
  }
  
  /**
   * A faster binary search algorithm for sorting a list.
   * This algorithm works by actually knowing the values 
   * and making smart decisions about how far to jump in 
   * the list based on those values.  Which is why this
   * can not take in a comparable interface like Collections
   * does.  This was adapted from code posted from this 
   * blog post: http://ochafik.com/blog/?p=106
   * 
   * @param list to be searched through
   * @param key delay value to search for
   * @return index where found, or -(insertion point) - 1 if not found
   */
  public static int binarySearch(List<? extends Delayed> list, 
                                 Delayed key) {
    return binarySearch(list, key, 
                        list instanceof RandomAccess);
  }
  
  /**
   * A faster binary search algorithm for sorting a list.
   * This algorithm works by actually knowing the values 
   * and making smart decisions about how far to jump in 
   * the list based on those values.  Which is why this
   * can not take in a comparable interface like Collections
   * does.  This was adapted from code posted from this 
   * blog post: http://ochafik.com/blog/?p=106
   * 
   * @param list to be searched through
   * @param key delay value to search for
   * @param randomAccessList boolean representing if optimizations should be done for list that have cheap random access
   * @return index where found, or -(insertion point) - 1 if not found
   */
  public static int binarySearch(List<? extends Delayed> list, 
                                 Delayed key, boolean randomAccessList) {
    if (list.isEmpty()) {
      return -1;
    }
    
    final int absoluteMin = 0;
    final int absoluteMax = list.size() - 1;
    
    long keyValue = key.getDelay(TimeUnit.MILLISECONDS);
    int min = absoluteMin;
    int max = absoluteMax;
    long minVal = list.get(absoluteMin).getDelay(TimeUnit.MILLISECONDS);
    long maxVal = list.get(absoluteMax).getDelay(TimeUnit.MILLISECONDS);
    
    int nPreviousSteps = 1;
    while (true) {
      if (keyValue <= minVal) {
        return keyValue == minVal ? min : -1 - min;
      } else if (keyValue >= maxVal) {
        return keyValue == maxVal ? max : -2 - max;
      }
      
      int pivot;
      // A typical binarySearch algorithm uses pivot = (min + max) / 2.
      // The pivot we use here tries to be smarter and to choose a pivot close to the expected location of the key.
      // This reduces dramatically the number of steps needed to get to the key.
      // However, it does not work well with a logaritmic distribution of values.
      // When the key is not found quickly the smart way, we switch to the standard pivot.
      if (nPreviousSteps > 5) {
        pivot = (min + max) >> 1;
        // stop increasing nPreviousSteps from now on
      } else {
        // We cannot do the following operations in int precision, because there might be overflows.
        // using a float is better performing than using a long (even on 64bit)
        pivot = min + (int)((keyValue - (float)minVal) / (maxVal - (float)minVal) * (max - min));
        nPreviousSteps++;
      }
      
      long pivotVal = list.get(pivot).getDelay(TimeUnit.MILLISECONDS);
      
      if (keyValue > pivotVal) {
        min = pivot + 1;
        if (min > absoluteMax) {
          return absoluteMax + 1;
        }
        minVal = list.get(min).getDelay(TimeUnit.MILLISECONDS);
        if (randomAccessList) {
          max--;
          maxVal = list.get(max).getDelay(TimeUnit.MILLISECONDS);
        }
      } else if (keyValue < pivotVal) {
        max = pivot - 1;
        if (max < absoluteMin) {
          return absoluteMin;
        }
        maxVal = list.get(max).getDelay(TimeUnit.MILLISECONDS);
        if (randomAccessList) {
          min++;
          minVal = list.get(min).getDelay(TimeUnit.MILLISECONDS);
        }
      } else {
        return pivot;
      }
    }
  }
}
