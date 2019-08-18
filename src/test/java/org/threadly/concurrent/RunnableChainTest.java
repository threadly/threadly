package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.StackSuppressedRuntimeException;

@SuppressWarnings("javadoc")
public class RunnableChainTest extends ThreadlyTester {
  @BeforeClass
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  private static final int FAIL_INDEX = 2;
  
  @Test
  public void constructorTest() {
    new RunnableChain(false, DoNothingRunnable.instance()).run();
    new RunnableChain(true, DoNothingRunnable.instance()).run();
  }
  
  @Test
  public void exceptionStopsChainTest() {
    List<TestRunnable> list = new ArrayList<>(TEST_QTY);
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
    List<TestRunnable> list = new ArrayList<>(TEST_QTY);
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
        throw new StackSuppressedRuntimeException("Test failure exception");
      }
    }
  }
}
