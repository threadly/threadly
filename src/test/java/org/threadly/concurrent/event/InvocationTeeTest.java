package org.threadly.concurrent.event;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class InvocationTeeTest {
  @Test
  public void invokTest() {
    TestRunnable tr = new TestRunnable();
    Runnable r = InvocationTee.tee(Runnable.class, null, tr);
    r.run();
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void invokTwiceTest() {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    Runnable r = InvocationTee.tee(Runnable.class, tr1, null, tr2, null);
    r.run();
    r.run();
    
    assertEquals(2, tr1.getRunCount());
    assertEquals(2, tr2.getRunCount());
  }
}
