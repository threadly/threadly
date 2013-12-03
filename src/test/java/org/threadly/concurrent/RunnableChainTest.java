package org.threadly.concurrent;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.threadly.concurrent.lock.LockFactory;
import org.threadly.concurrent.lock.NativeLockFactory;
import org.threadly.test.concurrent.TestRunnable;

@SuppressWarnings("javadoc")
public class RunnableChainTest {
  private static final int RUNNABLE_COUNT = 5;
  private static final int FAIL_INDEX = 2;
  
  @Test
  public void constructorTest() {
    new RunnableChain(false, null).run();
    new RunnableChain(true, null).run();
  }
  
  @Test
  public void exceptionStopsChainTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      list.add(new ChainRunnable(i == FAIL_INDEX));
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
        assertEquals(0, tr.getRunCount());
      } else {
        assertEquals(1, tr.getRunCount());
      }
    }
  }
  
  @Test
  public void runAllProvidedTest() {
    List<TestRunnable> list = new ArrayList<TestRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      list.add(new ChainRunnable(i == FAIL_INDEX));
    }

    RunnableChain chain = new RunnableChain(false, list);
    try {
      chain.run();
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      TestRunnable tr = list.get(i);
      assertEquals(1, tr.getRunCount());
    }
  }
  
  @Test
  public void runWithLockFactoryTest() {
    List<ChainRunnable> list = new ArrayList<ChainRunnable>(RUNNABLE_COUNT);
    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      list.add(new ChainRunnable(i == FAIL_INDEX));
    }
    
    LockFactory lf = new NativeLockFactory();

    RunnableChain chain = new RunnableChain(false, list);
    try {
      chain.run(lf);
    } catch (RuntimeException expected) {
      // ignore expected exception
    }

    for (int i = 0; i < RUNNABLE_COUNT; i++) {
      ChainRunnable cr = list.get(i);
      assertEquals(1, cr.getRunCount());
      assertTrue(cr.factorySetAtRuntime);
    }
  }
  
  private class ChainRunnable extends TestRunnable {
    private final boolean fail;
    private volatile boolean factorySetAtRuntime;
    
    private ChainRunnable(boolean fail) {
      this.fail = fail;
    }
    
    @Override
    protected void handleRunStart() {
      factorySetAtRuntime = factory != null;
      if (fail) {
        throw new RuntimeException("Test failure exception");
      }
    }
    
  }
}
