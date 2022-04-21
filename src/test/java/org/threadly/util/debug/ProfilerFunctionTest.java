package org.threadly.util.debug;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class ProfilerFunctionTest extends ThreadlyTester {
  @Test
  public void equalsSameObjectTest() {
    Profiler.WitnessedFunction f = new Profiler.WitnessedFunction("foo", "bar");
    
    assertTrue(f.equals(f));
  }
  
  @Test
  public void equalsEquivelentTest() {
    Profiler.WitnessedFunction f1 = new Profiler.WitnessedFunction("foo", "bar");
    Profiler.WitnessedFunction f2 = new Profiler.WitnessedFunction("foo", "bar");
    
    assertTrue(f1.equals(f2));
  }
  
  @Test
  public void equalsFalseTest() {
    Profiler.WitnessedFunction f1 = new Profiler.WitnessedFunction("foo", "bar");
    Profiler.WitnessedFunction f2 = new Profiler.WitnessedFunction("foo", "foo");
    Profiler.WitnessedFunction f3 = new Profiler.WitnessedFunction("bar", "bar");
    
    assertFalse(f1.equals(f2));
    assertFalse(f1.equals(f3));
    assertFalse(f2.equals(f1));
    assertFalse(f2.equals(f3));
  }
  
  @Test
  public void equalsDiffObjectTest() {
    Profiler.WitnessedFunction f = new Profiler.WitnessedFunction("foo", "bar");
    
    assertFalse(f.equals(new Object()));
  }
}
