package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ContainerHelperTest {
  
  @Test
  public void twoDifferentRunnableTest() {
    assertFalse(ContainerHelper.isContained(new TestRunnable(), new TestRunnable()));
  }
  
  @Test
  public void sameRunnableTest() {
    TestRunnable tr = new TestRunnable();
    assertTrue(ContainerHelper.isContained(tr, tr));
  }
  
  @Test
  public void containedRunnableTest() {
    TestRunnable tr = new TestRunnable();
    assertTrue(ContainerHelper.isContained(new TestRunnableContainer(tr), tr));
  }
  
  @Test
  public void containedCallableTest() {
    TestCallable tc = new TestCallable();
    assertTrue(ContainerHelper.isContained(new TestCallableContainer(tc), tc));
  }
  
  @Test
  public void notContainedCallableTest() {
    TestCallable tc = new TestCallable();
    assertFalse(ContainerHelper.isContained(new TestRunnableContainer(new TestRunnable()), tc));
    assertFalse(ContainerHelper.isContained(new TestRunnable(), tc));
  }
  
  private static class TestRunnableContainer implements RunnableContainerInterface, Runnable {
    private final Runnable r;
    
    private TestRunnableContainer(Runnable r) {
      this.r = r;
    }
    
    @Override
    public Runnable getContainedRunnable() {
      return r;
    }

    @Override
    public void run() {
      // ignored
    }
  }
  
  @SuppressWarnings("rawtypes")
  private static class TestCallableContainer implements CallableContainerInterface, Runnable {
    private final Callable<?> c;
    
    private TestCallableContainer(Callable<?> c) {
      this.c = c;
    }

    @Override
    public Callable<?> getContainedCallable() {
      return c;
    }

    @Override
    public void run() {
      // ignored
    }
  }
}
