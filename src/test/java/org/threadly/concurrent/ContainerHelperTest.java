package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class ContainerHelperTest {
  @Test
  public void removeRunnableFromCollectionTest() {
    List<TestRunnableContainer> testRunnables = new ArrayList<TestRunnableContainer>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      testRunnables.add(new TestRunnableContainer(new TestRunnable()));
    }
    
    Runnable toRemove = testRunnables.get(TEST_QTY / 2).r;
    
    assertTrue(ContainerHelper.remove(testRunnables, toRemove));
  }
  
  @Test
  public void removeCallableFromCollectionTest() {
    List<TestRunnableContainer> testRunnables = new ArrayList<TestRunnableContainer>(TEST_QTY);
    Callable<?> toRemove = null;
    for (int i = 0; i < TEST_QTY; i++) {
      TestCallable tc = new TestCallable();
      if (TEST_QTY / 2 == i) {
        toRemove = tc;
      }
      testRunnables.add(new TestRunnableContainer(new TestCallableContainer(tc)));
    }
    
    assertTrue(ContainerHelper.remove(testRunnables, toRemove));
  }
  
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
  
  @Test
  public void getContainedRunnablesTest() {
    List<TestRunnableContainer> containers = new ArrayList<TestRunnableContainer>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      containers.add(new TestRunnableContainer(new TestRunnable()));
    }
    
    List<Runnable> resultList = ContainerHelper.getContainedRunnables(containers);
    assertEquals(containers.size(), resultList.size());
    
    Iterator<TestRunnableContainer> it = containers.iterator();
    while (it.hasNext()) {
      assertTrue(resultList.contains(it.next().r));
    }
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
