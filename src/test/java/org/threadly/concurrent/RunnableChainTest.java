package org.threadly.concurrent;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableChainTest {
  private static final int FAIL_INDEX = 2;
  
  @Test
  public void constructorTest() {
    new RunnableChain(false, null).run();
    new RunnableChain(true, null).run();
  }
  
  @Test
  public void exceptionStopsChainTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      list.add(new ChainRunnable(i == FAIL_INDEX));
    }

    RunnableChain chain = new RunnableChain(true, list);
    try {
      chain.run();
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = list.get(i);
      if (i > FAIL_INDEX) {
        assertEquals(0, tr.getRunCount());
      } else {
        assertEquals(1, tr.getRunCount());
      }
    }
  }
  
  @Test
  public void runAllProvidedTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(TEST_QTY);
    for (int i = 0; i < TEST_QTY; i++) {
      list.add(new ChainRunnable(i == FAIL_INDEX));
    }

    RunnableChain chain = new RunnableChain(false, list);
    try {
      chain.run();
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < TEST_QTY; i++) {
      TestRunnable tr = list.get(i);
      assertEquals(1, tr.getRunCount());
    }
  }
  
  private class ChainRunnable extends TestRunnable {
    private final boolean fail;
    
    private ChainRunnable(boolean fail) {
      this.fail = fail;
    }
    
    @Override
    public void handleRunStart() {
      if (fail) {
        throw new RuntimeException("Test failure exception");
      }
    }
  }
}
