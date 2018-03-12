package org.threadly.concurrent.event;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.Pair;
import org.threadly.util.StringUtils;
import org.threadly.util.SuppressedStackRuntimeException;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings("javadoc")
public class ListenerHelperTest {
  protected <T> ListenerHelper<T> makeListenerHelper(Class<T> listenerInterface) {
    return new ListenerHelper<T>(listenerInterface);
  }
  
  @Test
  @SuppressWarnings({ "unused", "unchecked", "rawtypes" })
  public void constructorFail() {
    try {
      new ListenerHelper(null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new ListenerHelper(ArrayList.class);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void getSubscribedListenersTest() {
    ListenerHelper<TestInterface> lh = makeListenerHelper(TestInterface.class);
    assertTrue(lh.getSubscribedListeners().isEmpty());
    TestImp ti = new TestImp();
    lh.addListener(ti);
    assertTrue(lh.getSubscribedListeners().contains(ti));
    lh.removeListener(ti);
    assertTrue(lh.getSubscribedListeners().isEmpty());
  }
  
  @Test
  public void getSubscribedListenersInThreadOnlyTest() {
    ListenerHelper<TestInterface> lh = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp();
    lh.addListener(ti);
    assertTrue(lh.getSubscribedListeners().contains(ti));
  }
  
  @Test
  public void getSubscribedListenersExecutorOnlyTest() {
    ListenerHelper<TestInterface> lh = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp();
    lh.addListener(ti, SameThreadSubmitterExecutor.instance());
    assertTrue(lh.getSubscribedListeners().contains(ti));
  }
  
  @Test
  public void getSubscribedListenersMixedExecutionTest() {
    ListenerHelper<TestInterface> lh = makeListenerHelper(TestInterface.class);
    TestImp ti1 = new TestImp();
    TestImp ti2 = new TestImp();
    lh.addListener(ti1);
    lh.addListener(ti2, SameThreadSubmitterExecutor.instance());
    assertTrue(lh.getSubscribedListeners().contains(ti1));
    assertTrue(lh.getSubscribedListeners().contains(ti2));
  }
  
  @Test
  public void addNullListenerTest() {
    makeListenerHelper(TestInterface.class).addListener(null);
    // no exception thrown
  }
  
  @Test
  public void addListenerTest() {
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp();
    ch.addListener(ti);
    
    assertEquals(1, ch.registeredListenerCount());
    assertTrue(Pair.containsLeft(ch.listeners, ti));
  }
  
  @Test
  public void addListenerWithExecutorTest() {
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp();
    Executor executor = new SameThreadSubmitterExecutor();
    ch.addListener(ti, executor);

    assertEquals(1, ch.registeredListenerCount());
    assertTrue(Pair.getRightFromLeft(ch.listeners, ti) == executor);
  }
  
  @Test
  public void addListenerFromCallTest() {
    int firstCallInt = 42;
    String firstCallStr = StringUtils.makeRandomString(10);
    int secondCallInt = 1337;
    String secondCallStr = StringUtils.makeRandomString(10);
    final TestImp addedListener = new TestImp();
    final ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp() {
      @Override
      public void call(int i, String s) {
        super.call(i, s);
        ch.addListener(addedListener);
      }
    };
    ch.addListener(ti);
    ch.addListener(new TestImp());
    ch.addListener(new TestImp());
    TestImp lastListener = new TestImp();
    ch.addListener(lastListener);
    
    ch.call().call(firstCallInt, firstCallStr);
    // verify the other listeners were called
    assertEquals(firstCallInt, lastListener.lastInt);
    assertEquals(firstCallStr, lastListener.lastString);

    assertEquals(5, ch.registeredListenerCount());
    
    // verify new listener can be called
    ch.call().call(secondCallInt, secondCallStr);
    assertEquals(secondCallInt, addedListener.lastInt);
    assertEquals(secondCallStr, addedListener.lastString);
  }
  
  @Test
  public void removeListenerTest() {
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp();
    
    assertFalse(ch.removeListener(null));
    
    ch.addListener(ti);
    assertFalse(ch.removeListener(null));
    assertFalse(ch.removeListener(new TestImp()));
    assertEquals(1, ch.registeredListenerCount());
    
    assertTrue(ch.removeListener(ti));
    assertEquals(0, ch.registeredListenerCount());
    
    ch.call().call(10, StringUtils.makeRandomString(10));
    // verify not called
    assertNull(ti.lastString);
  }
  
  @Test
  public void removeListenerFromCallTest() {
    int firstCallInt = 42;
    String firstCallStr = StringUtils.makeRandomString(10);
    int secondCallInt = 1337;
    String secondCallStr = StringUtils.makeRandomString(10);
    final TestImp removedListener = new TestImp();
    final ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    TestImp ti = new TestImp() {
      @Override
      public void call(int i, String s) {
        super.call(i, s);
        ch.removeListener(removedListener);
      }
    };
    ch.addListener(new TestImp());
    ch.addListener(new TestImp());
    ch.addListener(ti);
    ch.addListener(new TestImp());
    ch.addListener(new TestImp());
    ch.addListener(removedListener);
    ch.addListener(new TestImp());
    ch.addListener(new TestImp());
    TestImp lastListener = new TestImp();
    ch.addListener(lastListener);
    
    ch.call().call(firstCallInt, firstCallStr);
    // verify the other listeners were called
    assertEquals(firstCallInt, lastListener.lastInt);
    assertEquals(firstCallStr, lastListener.lastString);
    
    // verify listener was removed
    assertEquals(8, ch.registeredListenerCount());
    
    // call again and verify it does not call removed listener
    ch.call().call(secondCallInt, secondCallStr);
    assertEquals(firstCallInt, removedListener.lastInt);
    assertEquals(firstCallStr, removedListener.lastString);
  }
  
  @Test
  public void registeredListenerCountTest() {
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    
    assertEquals(0, ch.registeredListenerCount());
    
    ch.addListener(new TestImp());
    assertEquals(1, ch.registeredListenerCount());
    
    ch.addListener(new TestImp(), SameThreadSubmitterExecutor.instance());
    assertEquals(2, ch.registeredListenerCount());
  }
  
  @Test
  public void clearListenersTest() {
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    ch.addListener(new TestImp());
    ch.addListener(new TestImp(), SameThreadSubmitterExecutor.instance());
    assertEquals(2, ch.registeredListenerCount());
    
    ch.clearListeners();
    
    assertEquals(0, ch.registeredListenerCount());
  }
  
  @Test
  public void callTest() {
    callTest(false);
  }
  
  @Test
  public void callWithExecutorTest() {
    callTest(true);
  }
  
  private void callTest(boolean useExecutor) {
    int testInt = 10;
    String testStr = StringUtils.makeRandomString(10);
    List<TestImp> listeners = new ArrayList<TestImp>(TEST_QTY);
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    
    for (int i = 0; i < TEST_QTY; i++) {
      TestImp ti = new TestImp();
      listeners.add(ti);
      if (useExecutor) {
        ch.addListener(ti, SameThreadSubmitterExecutor.instance());
      } else {
        ch.addListener(ti);
      }
    }
    
    ch.call().call(testInt, testStr);
    
    Iterator<TestImp> it = listeners.iterator();
    while (it.hasNext()) {
      TestImp ti = it.next();
      assertEquals(testInt, ti.lastInt);
      assertEquals(testStr, ti.lastString);
    }
  }
  
  @Test
  public void callMultipleFunctionListenersTest() {
    int testInt = 10;
    String testStr = StringUtils.makeRandomString(10);
    List<TestMultipleFunctionImp> listeners = new ArrayList<TestMultipleFunctionImp>(TEST_QTY);
    ListenerHelper<TestMultipleFunctionInterface> ch = makeListenerHelper(TestMultipleFunctionInterface.class);
    
    for (int i = 0; i < TEST_QTY; i++) {
      TestMultipleFunctionImp ti = new TestMultipleFunctionImp();
      listeners.add(ti);
      ch.addListener(ti);
    }
    
    ch.call().call1(testInt);
    
    Iterator<TestMultipleFunctionImp> it = listeners.iterator();
    while (it.hasNext()) {
      TestMultipleFunctionImp ti = it.next();
      assertEquals(testInt, ti.lastCall1Int);
      assertNull(ti.lastCall2String);
    }
    
    ch.call().call2(testStr);
    
    it = listeners.iterator();
    while (it.hasNext()) {
      TestMultipleFunctionImp ti = it.next();
      assertEquals(testInt, ti.lastCall1Int);
      assertEquals(testStr, ti.lastCall2String);
    }
  }
  
  @Test
  public void listenerExceptionTest() {
    int testInt = 10;
    String testStr = StringUtils.makeRandomString(10);
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(teh);
    final RuntimeException e = new SuppressedStackRuntimeException();
    ListenerHelper<TestInterface> ch = makeListenerHelper(TestInterface.class);
    ch.addListener(new TestInterface() {
      @Override
      public void call(int i, String s) {
        throw e;
      }
    });
    TestImp ti = new TestImp();
    ch.addListener(ti);
    
    ch.call().call(testInt, testStr);
    
    // verify exception was handled
    assertEquals(1, teh.getCallCount());
    assertEquals(Thread.currentThread(), teh.getCalledWithThread());
    assertEquals(e, teh.getLastThrowable());
    
    // verify other listeners were called
    assertEquals(testInt, ti.lastInt);
    assertEquals(testStr, ti.lastString);
  }
  
  @Test (expected = RuntimeException.class)
  public void callFail() {
    @SuppressWarnings("rawtypes")
    ListenerHelper<List> ch = makeListenerHelper(List.class);
    ch.call().get(0);
  }
  
  protected interface TestInterface {
    public void call(int i, String s);
  }
  
  protected interface TestMultipleFunctionInterface {
    public void call1(int i);
    public void call2(String s);
  }
  
  protected static class TestImp implements TestInterface {
    private int lastInt = -1;
    private String lastString = null;
    
    @Override
    public void call(int i, String s) {
      lastInt = i;
      lastString = s;
    }
  }
  
  protected static class TestMultipleFunctionImp implements TestMultipleFunctionInterface {
    private int lastCall1Int = -1;
    private String lastCall2String = null;
    
    @Override
    public void call1(int i) {
      lastCall1Int = i;
    }

    @Override
    public void call2(String s) {
      lastCall2String = s;
    }
  }
}
