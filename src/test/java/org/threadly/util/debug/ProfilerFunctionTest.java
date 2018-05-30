package org.threadly.util.debug;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class ProfilerFunctionTest extends ThreadlyTester {
  @Test
  public void equalsSameObjectTest() {
    Profiler.Function f = new Profiler.Function("foo", "bar");
    
    assertTrue(f.equals(f));
  }
  
  @Test
  public void equalsEquivelentTest() {
    Profiler.Function f1 = new Profiler.Function("foo", "bar");
    Profiler.Function f2 = new Profiler.Function("foo", "bar");
    
    assertTrue(f1.equals(f2));
  }
  
  @Test
  public void equalsFalseTest() {
    Profiler.Function f1 = new Profiler.Function("foo", "bar");
    Profiler.Function f2 = new Profiler.Function("foo", "foo");
    Profiler.Function f3 = new Profiler.Function("bar", "bar");
    
    assertFalse(f1.equals(f2));
    assertFalse(f1.equals(f3));
    assertFalse(f2.equals(f1));
    assertFalse(f2.equals(f3));
  }
  
  @Test
  public void equalsDiffObjectTest() {
    Profiler.Function f = new Profiler.Function("foo", "bar");
    
    assertFalse(f.equals(new Object()));
  }
}
