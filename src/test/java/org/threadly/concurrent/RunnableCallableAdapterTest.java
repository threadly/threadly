package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableCallableAdapterTest {
  @Test
  public void constructorTest() {
    TestRunnable tr = new TestRunnable();
    Object result = new Object();
    RunnableCallableAdapter<?> rca = new RunnableCallableAdapter<Object>(tr, result);
    
    assertTrue(tr == rca.runnable);
    assertTrue(result == rca.result);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new RunnableCallableAdapter<Object>(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    RunnableCallableAdapter<?> rca = new RunnableCallableAdapter<Object>(tr);
    
    assertTrue(tr == rca.getContainedRunnable());
  }
  
  @Test
  public void callTest() {
    TestRunnable tr = new TestRunnable();
    Object result = new Object();
    RunnableCallableAdapter<?> rca = new RunnableCallableAdapter<Object>(tr, result);
    
    assertTrue(result == rca.call());
    assertTrue(tr.ranOnce());
  }
}
