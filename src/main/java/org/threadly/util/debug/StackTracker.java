package org.threadly.util.debug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.threadly.util.Pair;

/**
 * This thread safe class allows you to record stacks in the code so that you can understand HOW 
 * something is being called.  This is similar to {@link Profiler} except there is no threading 
 * concept.  Instead it is focused on the stack traces only and can be accessed concurrently.
 */
public class StackTracker {
  private static final Function<Object, LongAdder> ADDER_FACTORY = (ignored) -> new LongAdder();
  
  private final Map<ComparableTrace, LongAdder> traces = new ConcurrentHashMap<>();
  
  /**
   * Record the current stack into the internally monitored traces.  The call into 
   * {@link StackTracker} wont be included in the resulting stack.
   */
  public void recordStack() {
    traces.computeIfAbsent(new ComparableTrace(Thread.currentThread().getStackTrace()), ADDER_FACTORY)
          .increment();
  }
  
  /**
   * Check how many unique stack traces have been recorded.
   * 
   * @return A list with all the stack traces, de-duplicated, with the counts they were witnessed.
   */
  public List<Pair<StackTraceElement[], Long>> dumpStackCounts() {
    List<Pair<StackTraceElement[], Long>> result = new ArrayList<>(traces.size());
    for (Map.Entry<ComparableTrace, LongAdder> e : traces.entrySet()) {
      result.add(new Pair<>(Arrays.copyOfRange(e.getKey().elements, // remove top two elements
                                               2, e.getKey().elements.length), 
                            e.getValue().sum()));
    }
    return result;
  }
  
  /**
   * Reset all stored data to allow a new capture.
   */
  public void reset() {
    traces.clear();
  }
  
  /**
   * Class which represents a stack trace.  The is used so we can track how many times a given 
   * stack is seen.
   * 
   * @since 5.20
   */
  protected static class ComparableTrace implements Comparable<ComparableTrace> {
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
}
