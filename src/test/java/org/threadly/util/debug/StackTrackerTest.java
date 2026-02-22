package org.threadly.util.debug;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.Pair;

@SuppressWarnings("javadoc")
public class StackTrackerTest extends ThreadlyTester {
  private StackTracker tracker;
  
  @BeforeEach
  public void setup() {
    tracker = new StackTracker();
  }
  
  @AfterEach
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
