package org.threadly.concurrent;

import java.util.Iterator;
import java.util.List;

/**
 * <p>Counterpart to {@link org.threadly.util.ListUtils} except for use with the 
 * {@link DelayedTaskInterface}.  Eventually this class will be removed when 
 * {@link org.threadly.util.ListUtils} can be written more generically easily.</p>
 * 
 * @author jent
 * @since 4.2.0
 */
// TODO - Once on java 8 update ListUtils to remove the code duplication from this class
class TaskListUtils {
  /**
   * Counterpart to {@link org.threadly.util.ListUtils#getInsertionEndIndex(List, long, boolean)}.  
   * Time argument should be in an absolute time in reference to 
   * {@link org.threadly.util.Clock#accurateForwardProgressingMillis()}.
   * 
   * @param list List to examine to find insertion index from
   * @param insertionRunTime Absolute time for insertion
   * @return Index provided runtime should be inserted into
   */
  protected static int getInsertionEndIndex(List<? extends DelayedTaskInterface> list, 
                                            long insertionRunTime) {
    int searchResult = binarySearch(list, insertionRunTime);
    if (searchResult >= 0) {
      Iterator<? extends DelayedTaskInterface> it = list.listIterator(searchResult);
      while (it.hasNext() && it.next().getRunTime() <= insertionRunTime) {
        searchResult++;
      }
      return searchResult;
    } else {
      return Math.abs(searchResult) - 1;
    }
  }
  
  private static int binarySearch(List<? extends DelayedTaskInterface> list, long insertionRunTime) {
    if (list.isEmpty()) {
      return -1;
    }
    
    final int absoluteMin = 0;
    final int absoluteMax = list.size() - 1;
    
    int min = absoluteMin;
    int max = absoluteMax;
    long minVal = list.get(absoluteMin).getRunTime();
    long maxVal = list.get(absoluteMax).getRunTime();
    
    int nPreviousSteps = 0;
    while (true) {
      if (insertionRunTime <= minVal) {
        return insertionRunTime == minVal ? min : -1 - min;
      } else if (insertionRunTime >= maxVal) {
        return insertionRunTime == maxVal ? max : -2 - max;
      }
      
      int pivot;
      // A typical binarySearch algorithm uses pivot = (min + max) / 2.
      // The pivot we use here tries to be smarter and to choose a pivot 
      // close to the expected location of the key. This reduces dramatically 
      // the number of steps needed to get to the key.  However, it does not 
      // work well with a logarithmic distribution of values. When the key is 
      // not found quickly the smart way, we switch to the standard pivot.
      if (nPreviousSteps > 2) {
        pivot = (min + max) >> 1;
        // stop increasing nPreviousSteps from now on
      } else {
        // We cannot do the following operations in int precision, because there might be overflows.
        // using a float is better performing than using a long (even on 64bit)
        pivot = min + (int)((insertionRunTime - (float)minVal) / (maxVal - (float)minVal) * (max - min));
        nPreviousSteps++;
      }
      
      long pivotVal = list.get(pivot).getRunTime();
      
      if (insertionRunTime > pivotVal) {
        min = pivot + 1;
        if (min > absoluteMax) {
          return absoluteMax + 1;
        }
        minVal = list.get(min).getRunTime();
        max--;
        maxVal = list.get(max).getRunTime();
      } else if (insertionRunTime < pivotVal) {
        max = pivot - 1;
        if (max < absoluteMin) {
          return absoluteMin;
        }
        maxVal = list.get(max).getRunTime();
        min++;
        minVal = list.get(min).getRunTime();
      } else {
        return pivot;
      }
    }
  }
}
