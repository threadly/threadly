package org.threadly.concurrent;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;

@SuppressWarnings("javadoc")
public class ListenerHelperTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setDefaultUncaughtExceptionHandler();
  }
  
  private ListenerHelper onceHelper;
  private ListenerHelper repeatedHelper;
  
  @Before
  public void setup() {
    onceHelper = new ListenerHelper(true);
    repeatedHelper = new ListenerHelper(false);
  }
  
  @After
  public void tearDown() {
    onceHelper = null;
    repeatedHelper = null;
  }
  
  @Test
  public void runListenerNoExecutorTest() {
    TestRunnable tr = new TestRunnable();
    onceHelper.runListener(tr, null, true);
    
    assertTrue(tr.ranOnce());
    assertTrue(Thread.currentThread() == tr.lastRanThread);
  }
  
  @Test
  public void runListenerExecutorTest() {
    PriorityScheduledExecutor executor = new StrictPriorityScheduledExecutor(1, 1, 100);
    try {
      TestRunnable tr = new TestRunnable();
      onceHelper.runListener(tr, executor, true);
      tr.blockTillFinished();
      
      assertTrue(tr.ranOnce());
      assertTrue(Thread.currentThread() != tr.lastRanThread);
    } finally {
      executor.shutdownNow();
    }
  }
  
  @Test
  public void runListenerCatchExecptionTest() {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    onceHelper.runListener(tr, null, false);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test (expected = RuntimeException.class)
  public void runListenerThrowExecptionTest() {
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        throw new RuntimeException();
      }
    };
    onceHelper.runListener(tr, null, true);
    fail("Execption should have thrown");
  }
  
  @Test
  public void registeredListenerCountTest() {
    assertEquals(0, onceHelper.registeredListenerCount());
    assertEquals(0, repeatedHelper.registeredListenerCount());
    
    onceHelper.addListener(new TestRunnable());
    repeatedHelper.addListener(new TestRunnable());
    
    assertEquals(1, onceHelper.registeredListenerCount());
    assertEquals(1, repeatedHelper.registeredListenerCount());
    
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    
    assertEquals(0, onceHelper.registeredListenerCount());
    assertEquals(1, repeatedHelper.registeredListenerCount());
  }
  
  @Test
  public void addAndCallListenersTest() {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    onceHelper.addListener(onceTR);
    repeatedHelper.addListener(repeatedTR);
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    
    assertTrue(onceTR.ranOnce());
    assertTrue(repeatedTR.ranOnce());
    
    repeatedHelper.callListeners();
    
    assertTrue(onceTR.ranOnce());
    assertEquals(2, repeatedTR.getRunCount());
  }
  
  @Test
  public void addAfterCalledTest() {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    onceHelper.addListener(onceTR);
    repeatedHelper.addListener(repeatedTR);
    
    assertTrue(onceTR.ranOnce());
    assertFalse(repeatedTR.ranOnce());
    
    repeatedHelper.callListeners();
    
    assertTrue(onceTR.ranOnce());
    assertTrue(repeatedTR.ranOnce());
  }
  
  @Test
  public void listenerExceptionAfterCallTest() {
    TestRuntimeFailureRunnable listener = new TestRuntimeFailureRunnable();

    onceHelper.callListeners();
    
    try {
      onceHelper.addListener(listener);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      // expected
    }
    
    assertTrue(listener.ranOnce());
  }
  
  @Test (expected = RuntimeException.class)
  public void callListenersFail() {
    onceHelper.callListeners();
    // should fail on subsequent calls
    onceHelper.callListeners();
  }
  
  @Test
  public void removeListenerTest() {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    
    assertFalse(onceHelper.removeListener(onceTR));
    assertFalse(repeatedHelper.removeListener(repeatedTR));
    
    onceHelper.addListener(onceTR);
    repeatedHelper.addListener(repeatedTR);

    // should be false for the opposite
    assertFalse(onceHelper.removeListener(repeatedTR));
    assertFalse(repeatedHelper.removeListener(onceTR));
    
    assertTrue(onceHelper.removeListener(onceTR));
    assertTrue(repeatedHelper.removeListener(repeatedTR));
  }
  
  @Test
  public void removeListenerAfterCallTest() {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    
    assertFalse(onceHelper.removeListener(onceTR));
    assertFalse(repeatedHelper.removeListener(repeatedTR));
    
    onceHelper.addListener(onceTR);
    repeatedHelper.addListener(repeatedTR);
    
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    
    assertFalse(onceHelper.removeListener(onceTR));
    assertTrue(repeatedHelper.removeListener(repeatedTR));
  }
  
  @Test
  public void clearListenersTest() {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    onceHelper.addListener(onceTR);
    repeatedHelper.addListener(repeatedTR);
    
    onceHelper.clearListeners();
    repeatedHelper.clearListeners();
    
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    
    assertFalse(onceTR.ranOnce());
    assertFalse(repeatedTR.ranOnce());
  }
  
  @Test
  public void addListenerFromCallingThread() {
    final TestRunnable addedTR = new TestRunnable();
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        repeatedHelper.addListener(addedTR);
      }
    };
    repeatedHelper.addListener(tr);
    repeatedHelper.addListener(new TestRunnable());
    
    repeatedHelper.callListeners();
    
    assertTrue(tr.ranOnce());
    assertEquals(0, addedTR.getRunCount());
    
    repeatedHelper.callListeners();
    
    assertEquals(2, tr.getRunCount());
    assertEquals(1, addedTR.getRunCount());
  }
  
  private static class TestRunnable extends org.threadly.test.concurrent.TestRunnable {
    private volatile Thread lastRanThread = null;
    
    @Override
    public void handleRunStart() {
      lastRanThread = Thread.currentThread();
    }
  }
}
