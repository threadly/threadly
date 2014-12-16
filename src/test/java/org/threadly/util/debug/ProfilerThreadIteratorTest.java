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
  public void tearDown() {
    ti = null;
  }
  
  @Test
  public void refreshThreadsTest() {
    ti.refreshThreads();
    
    assertTrue(ti.threads.length > 0);
    assertTrue(ti.enumerateCount > 0);
    assertEquals(0, ti.currentIndex);
  }
  
  @Test
  public void refreshThreadsNullPreviousThreadsTest() {
    ti.refreshThreads();
    Thread t = new Thread();
    ti.threads = new Thread[ti.enumerateCount * 10];
    for (int i = 0; i < ti.threads.length; i++) {
      ti.threads[i] = t;
    }
    ti.enumerateCount = ti.threads.length;
    
    ti.refreshThreads();
    
    for (int i = ti.enumerateCount; i < ti.threads.length; i++) {
      assertNull(ti.threads[i]);
    }
  }
  
  @Test
  public void hasNextTest() {
    assertFalse(ti.hasNext());
    
    ti.refreshThreads();
    
    assertTrue(ti.hasNext());
    
    ti.currentIndex = ti.enumerateCount;
    
    assertFalse(ti.hasNext());
  }
  
  @Test
  public void nextTest() {
    ti.refreshThreads();
    Thread t = new Thread();
    ti.threads[0] = t;
    
    assertTrue(t == ti.next());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void removeFail() {
    ti.remove();
  }
}
