package org.threadly.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Utilities for getting some basic statistics out of numerical data collections.</p>
 * 
 * @author jent - Mike Jensen
 * @since 4.5.0
 */
public class StatisticsUtils {
  /**
   * Calculates the average from a collection of numeric values.
   * 
   * @param list List of numbers to average against
   * @return Zero if the list is empty, otherwise the average of the values inside the list
   */
  public static double getAverage(Collection<? extends Number> list) {
    if (list.isEmpty()) {
      return 0;
    }
    
    double totalTime = 0;
    Iterator<? extends Number> it = list.iterator();
    while (it.hasNext()) {
      totalTime += it.next().doubleValue();
    }
      
    return totalTime / list.size();
  }
  
  /**
   * Gets percentile values from a collection of numeric values.  This function is NOT dependent 
   * on the collection already being sorted.  This function accepts any decimal percentile between 
   * zero and one hundred, but requests for 99.9 and 99.99 may return the same result if the sample 
   * set is not large or varied enough.  There is no attempt to extrapolate trends, thus only real 
   * samples are returned.  
   * 
   * The returned map's keys correspond exactly to the percentiles provided.  Iterating over the 
   * returned map will iterate in order of the requested percentiles as well.
   * 
   * @param values A non-empty collection of numbers to examine for percentiles
   * @param percentiles Percentiles requested, any decimal values between 0 and 100 (inclusive)
   * @return Map with keys being the percentiles requested in the second argument
   * @param <T> Specific number type contained in the collection
   */
  public static <T extends Number> Map<Double, T> getPercentiles(Collection<? extends T> values, 
                                                                 double ... percentiles) {
    if (percentiles.length == 0) {
      throw new IllegalArgumentException("No percentiles requested");
    } else if (values.isEmpty()) {
      throw new IllegalArgumentException("No values provided to calculate against");
    }
    
    List<T> valuesCopy = new ArrayList<T>(values);
    Collections.sort(valuesCopy, new Comparator<Number>() {
      @Override
      public int compare(Number o1, Number o2) {
        double result = o1.doubleValue() - o2.doubleValue();
        if (result > 0) {
          return 1;
        } else if (result < 0) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    
    Map<Double, T> result = new LinkedHashMap<Double, T>();
    for (double p : percentiles) {
      if (p > 100 || p < 0) {
        throw new IllegalArgumentException("Percentile not in range of 0 to 100: " + p);
      }
      
      int index;
      if (p == 100) {
        index = valuesCopy.size() - 1;
      } else {
        index = (int)((p / 100.) * valuesCopy.size());
      }
      result.put(p, valuesCopy.get(index));
    }
    return result;
  }
}
