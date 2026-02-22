package org.threadly.util.debug;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.debug.Profiler.ThreadIterator;

@SuppressWarnings("javadoc")
public class ProfilerThreadIteratorTest extends ThreadlyTester {
  private ThreadIterator ti;
  
  @BeforeEach
  public void setup() {
    ti = new ThreadIterator();
  }
  
  @AfterEach
  public void cleanup() {
    ti = null;
  }
  
  @Test
  public void constructorTest() {
    assertNotNull(ti.it);
    assertTrue(ti.it.hasNext());
  }
  
  @Test
  public void removeFail() {
      assertThrows(UnsupportedOperationException.class, () -> {
      ti.remove();
      });
  }
}
