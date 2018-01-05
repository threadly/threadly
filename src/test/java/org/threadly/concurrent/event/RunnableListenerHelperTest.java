package org.threadly.concurrent.event;

import static org.junit.Assert.*;

import java.util.concurrent.Executor;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTestUtil;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.PriorityScheduler;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.StrictPriorityScheduler;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.util.SuppressedStackRuntimeException;

@SuppressWarnings("javadoc")
public class RunnableListenerHelperTest {
  @BeforeClass
  public static void setupClass() {
    ThreadlyTestUtil.setIgnoreExceptionHandler();
  }
  
  protected RunnableListenerHelper onceHelper;
  protected RunnableListenerHelper repeatedHelper;
  
  @Before
  public void setup() {
    onceHelper = new RunnableListenerHelper(true);
    repeatedHelper = new RunnableListenerHelper(false);
  }
  
  @After
  public void cleanup() {
    onceHelper = null;
    repeatedHelper = null;
  }
  
  @Test
  public void getSubscribedListenersTest() {
    assertTrue(onceHelper.getSubscribedListeners().isEmpty());
    TestRunnable tr = new TestRunnable();
    onceHelper.addListener(tr);
    assertTrue(onceHelper.getSubscribedListeners().contains(tr));
    onceHelper.removeListener(tr);
    assertTrue(onceHelper.getSubscribedListeners().isEmpty());
  }
  
  @Test
  public void getSubscribedListenersInThreadOnlyTest() {
    TestRunnable tr = new TestRunnable();
    onceHelper.addListener(tr);
    assertTrue(onceHelper.getSubscribedListeners().contains(tr));
  }
  
  @Test
  public void getSubscribedListenersExecutorOnlyTest() {
    TestRunnable tr = new TestRunnable();
    onceHelper.addListener(tr, SameThreadSubmitterExecutor.instance());
    assertTrue(onceHelper.getSubscribedListeners().contains(tr));
  }
  
  @Test
  public void getSubscribedListenersMixedExecutionTest() {
    TestRunnable tr1 = new TestRunnable();
    TestRunnable tr2 = new TestRunnable();
    onceHelper.addListener(tr1);
    onceHelper.addListener(tr2, SameThreadSubmitterExecutor.instance());
    assertTrue(onceHelper.getSubscribedListeners().contains(tr1));
    assertTrue(onceHelper.getSubscribedListeners().contains(tr2));
  }
  
  @Test
  public void addNullListenerTest() {
    onceHelper.addListener(null);
    repeatedHelper.addListener(null);
    // no exception thrown
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
    PriorityScheduler executor = new StrictPriorityScheduler(1);
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
        throw new SuppressedStackRuntimeException();
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
        throw new SuppressedStackRuntimeException();
      }
    };
    onceHelper.runListener(tr, null, true);
    fail("Execption should have thrown");
  }
  
  @Test
  public void registeredListenerCountTest() {
    assertEquals(0, onceHelper.registeredListenerCount());
    assertEquals(0, repeatedHelper.registeredListenerCount());
    
    onceHelper.addListener(DoNothingRunnable.instance());
    repeatedHelper.addListener(DoNothingRunnable.instance());
    
    assertEquals(1, onceHelper.registeredListenerCount());
    assertEquals(1, repeatedHelper.registeredListenerCount());
    
    onceHelper.callListeners();
    repeatedHelper.callListeners();
    
    assertEquals(0, onceHelper.registeredListenerCount());
    assertEquals(1, repeatedHelper.registeredListenerCount());
    

    repeatedHelper.addListener(DoNothingRunnable.instance(), SameThreadSubmitterExecutor.instance());
    
    assertEquals(2, repeatedHelper.registeredListenerCount());
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
    removeListenerTest(null);
  }
  
  @Test
  public void removeExecutorListenerTest() {
    removeListenerTest(SameThreadSubmitterExecutor.instance());
  }
  
  private void removeListenerTest(Executor executor) {
    TestRunnable onceTR = new TestRunnable();
    TestRunnable repeatedTR = new TestRunnable();
    
    assertFalse(onceHelper.removeListener(onceTR));
    assertFalse(repeatedHelper.removeListener(repeatedTR));
    
    onceHelper.addListener(onceTR, executor);
    repeatedHelper.addListener(repeatedTR, executor);

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
  public void removeListenerFromCallingThreadTest() {
    removeListenerFromCallingThreadTest(null);
  }
  
  @Test
  public void removeExecutorListenerFromCallingThreadTest() {
    removeListenerFromCallingThreadTest(SameThreadSubmitterExecutor.instance());
  }
  
  private void removeListenerFromCallingThreadTest(Executor executor) {
    final TestRunnable removedRunnable = new TestRunnable();
    repeatedHelper.addListener(new TestRunnable());
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(new Runnable() {
      @Override
      public void run() {
        repeatedHelper.removeListener(removedRunnable);
      }
    }, executor);
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(removedRunnable, executor);
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(new TestRunnable(), executor);
    repeatedHelper.addListener(new TestRunnable());
    
    repeatedHelper.callListeners();
    
    // call again and verify it did not run again
    repeatedHelper.callListeners();
    assertEquals(1, removedRunnable.getRunCount());
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
    addListenerFromCallingThread(null);
  }
  
  @Test
  public void addExecutorListenerFromCallingThread() {
    addListenerFromCallingThread(SameThreadSubmitterExecutor.instance());
  }
  
  private void addListenerFromCallingThread(Executor executor) {
    final TestRunnable addedTR = new TestRunnable();
    TestRunnable tr = new TestRunnable() {
      @Override
      public void handleRunFinish() {
          repeatedHelper.addListener(addedTR, executor);
      }
    };
    repeatedHelper.addListener(tr, executor);
    repeatedHelper.addListener(DoNothingRunnable.instance(), executor);
    
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
