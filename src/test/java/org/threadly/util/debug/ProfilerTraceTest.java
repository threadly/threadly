package org.threadly.util.debug;

import static org.junit.Assert.*;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ProfilerTraceTest {
  @Test
  public void equalsSameObjectTest() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack);
    
    assertTrue(t1.equals(t1));
  }
  
  @Test
  public void equalsEquivelentTest() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack);
    Profiler.Trace t2 = new Profiler.Trace(stack);
    
    assertTrue(t1.equals(t2));
  }
  
  @Test
  public void equalsDiffStackTest() {
    StackTraceElement[] stack1 = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack1);
    StackTraceElement[] stack2 = new StackTraceElement[stack1.length - 1];
    System.arraycopy(stack1, 0, stack2, 0, stack2.length);
    Profiler.Trace t2 = new Profiler.Trace(stack2);
    
    assertFalse(t1.equals(t2));
  }
  
  @Test
  public void equalsDiffObjectTest() {
    StackTraceElement[] stack1 = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack1);
    
    assertFalse(t1.equals(new Object()));
  }
  
  @Test
  public void compareToEqualsTest() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack);
    Profiler.Trace t2 = new Profiler.Trace(stack);
    
    assertEquals(0, t1.compareTo(t2));
  }
  
  @Test
  public void compareToNotEqualsTest() {
    StackTraceElement[] stack1 = Thread.currentThread().getStackTrace();
    Profiler.Trace t1 = new Profiler.Trace(stack1);
    StackTraceElement[] stack2 = new StackTraceElement[stack1.length - 1];
    System.arraycopy(stack1, 0, stack2, 0, stack2.length);
    Profiler.Trace t2 = new Profiler.Trace(stack2);
    
    assertFalse(t1.compareTo(t2) == 0);
  }
}
