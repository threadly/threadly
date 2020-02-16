package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableCallableAdapterTest extends ThreadlyTester {
  @Test
  public void adaptTest() {
    TestRunnable tr = new TestRunnable();
    Object result = new Object();
    RunnableCallableAdapter<?> rca = (RunnableCallableAdapter<?>)RunnableCallableAdapter.adapt(tr, result);
    
    assertTrue(tr == rca.runnable);
    assertTrue(result == rca.result);
  }
  
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new RunnableCallableAdapter<>(null, null);
    fail("Exception should have thrown");
  }

  @Test (expected = IllegalArgumentException.class)
  public void adaptFail() {
    RunnableCallableAdapter.adapt(null, null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void getContainedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    RunnableCallableAdapter<?> rca = (RunnableCallableAdapter<?>)RunnableCallableAdapter.adapt(tr, null);
    
    assertTrue(tr == rca.getContainedRunnable());
  }
  
  @Test
  public void callTest() {
    TestRunnable tr = new TestRunnable();
    Object result = new Object();
    RunnableCallableAdapter<?> rca = (RunnableCallableAdapter<?>)RunnableCallableAdapter.adapt(tr, result);
    
    assertTrue(result == rca.call());
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void adaptDoNothingTest() {
    assertTrue(RunnableCallableAdapter.adapt(DoNothingRunnable.instance(), null) == 
               RunnableCallableAdapter.adapt(DoNothingRunnable.instance(), null));
    assertFalse(RunnableCallableAdapter.<Object>adapt(DoNothingRunnable.instance(), this) == 
                RunnableCallableAdapter.adapt(DoNothingRunnable.instance(), null));
  }
}
