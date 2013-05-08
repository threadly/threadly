package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class RunnableChainTest {
  private static final int RUNNABLE_COUNT = 5;
  private static final int FAIL_INDEX = 2;
  
  @Test
  public void exceptionStopsChainTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      list.add(new TestRunnable(i == FAIL_INDEX));
    }

    RunnableChain chain = new RunnableChain(true, list);
    try {
      chain.run();
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = list.get(i);
      if (i > FAIL_INDEX) {
        assertEquals(tr.ranCount, 0);
      } else {
        assertEquals(tr.ranCount, 1);
      }
    }
  }
  
  @Test
  public void runAllProvidedTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      list.add(new TestRunnable(i == FAIL_INDEX));
    }

    RunnableChain chain = new RunnableChain(false, list);
    try {
      chain.run();
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = list.get(i);
      assertEquals(tr.ranCount, 1);
    }
  }
  
  private class TestRunnable implements Runnable {
    private final boolean fail;
    private int ranCount;
    
    private TestRunnable(boolean fail) {
      this.fail = fail;
      ranCount = 0;
    }
    
    @Override
    public void run() {
      ranCount++;
      
      if (fail) {
        throw new RuntimeException("Test failure exception");
      }
    }
    
  }
}
