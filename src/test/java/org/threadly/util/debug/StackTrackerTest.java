package org.threadly.util.debug;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.Pair;

@SuppressWarnings("javadoc")
public class StackTrackerTest extends ThreadlyTester {
  private StackTracker tracker;
  
  @Before
  public void setup() {
    tracker = new StackTracker();
  }
  
  @After
  public void cleanup() {
    tracker = null;
  }
  
  @Test
  public void storeAndDumpSingleTest() {
    tracker.recordStack();
    
    List<Pair<StackTraceElement[], Long>> stacks = tracker.dumpStackCounts();
    
    assertEquals(1, stacks.size());
    assertEquals(1L, stacks.get(0).getRight().longValue());
  }
  
  @Test
  public void storeAndDumpDoubleTest() {
    for (int i = 0; i < 2; i++) {
      tracker.recordStack();
    }
    
    List<Pair<StackTraceElement[], Long>> stacks = tracker.dumpStackCounts();
    
    assertEquals(1, stacks.size());
    assertEquals(2L, stacks.get(0).getRight().longValue());
  }
  
  @Test
  public void storeAndDumpDoubleUniqueTest() {
    tracker.recordStack();
    tracker.recordStack();  // another line, so unique
    
    List<Pair<StackTraceElement[], Long>> stacks = tracker.dumpStackCounts();
    
    assertEquals(2, stacks.size());
    assertEquals(1L, stacks.get(0).getRight().longValue());
  }
}
