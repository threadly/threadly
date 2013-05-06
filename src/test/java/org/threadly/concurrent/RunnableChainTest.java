package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

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
        assertFalse(tr.ran);
      } else {
        assertTrue(tr.ran);
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
      assertTrue(tr.ran);
    }
  }
  
  private class TestRunnable implements Runnable {
    private final boolean fail;
    private boolean ran;
    
    private TestRunnable(boolean fail) {
      this.fail = fail;
      ran = false;
    }
    
    @Override
    public void run() {
      ran = true;
      
      if (fail) {
        throw new RuntimeException("Test failure exception");
      }
    }
    
  }
}
