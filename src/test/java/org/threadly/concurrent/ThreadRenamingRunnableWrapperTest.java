package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StringUtils;

@SuppressWarnings({"javadoc", "deprecation"})
public class ThreadRenamingRunnableWrapperTest {
  @Test
  public void renameReplaceAndResetTest() {
    final String originalName = Thread.currentThread().getName();
    final String newName = StringUtils.makeRandomString(5);
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() {
        assertEquals(newName, Thread.currentThread().getName());
      }
    };

    assertEquals(originalName, Thread.currentThread().getName());
    
    new ThreadRenamingRunnableWrapper(tr, newName, true).run();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void renamePrependAndResetTest() {
    final String originalName = Thread.currentThread().getName();
    final String newName = StringUtils.makeRandomString(5);
    
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunStart() {
        assertTrue(Thread.currentThread().getName().startsWith(newName));
        assertTrue(Thread.currentThread().getName().contains(originalName));
      }
    };

    assertEquals(originalName, Thread.currentThread().getName());
    
    new ThreadRenamingRunnableWrapper(tr, newName, false).run();
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    
    assertTrue(tr == new ThreadRenamingRunnableWrapper(tr, "foo", false).getContainedRunnable());
  }
}
