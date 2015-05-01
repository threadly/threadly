package org.threadly.util.debug;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.util.debug.Profiler.ThreadIterator;

@SuppressWarnings("javadoc")
public class ProfilerThreadIteratorTest {
  private ThreadIterator ti;
  
  @Before
  public void setup() {
    ti = new ThreadIterator();
  }
  
  @After
  public void cleanup() {
    ti = null;
  }
  
  @Test
  public void constructorTest() {
    assertNotNull(ti.it);
    assertTrue(ti.it.hasNext());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void removeFail() {
    ti.remove();
  }
}
