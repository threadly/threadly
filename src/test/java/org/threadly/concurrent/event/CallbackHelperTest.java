package org.threadly.concurrent.event;

import static org.junit.Assert.*;
import static org.threadly.TestConstants.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.TestUncaughtExceptionHandler;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class CallbackHelperTest {
  @SuppressWarnings({ "unused", "unchecked", "rawtypes" })
  @Test
  public void constructorFail() {
    try {
      new CallbackHelper(null);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
    try {
      new CallbackHelper(ArrayList.class);
      fail("Exception should have thrown");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
  
  @Test
  public void addCallbackTest() {
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    TestImp ti = new TestImp();
    ch.addCallback(ti);
    
    assertEquals(1, ch.registeredCallbackCount());
    assertTrue(ch.callbacks.containsKey(ti));
  }
  
  @Test
  public void addCallbackWithExecutorTest() {
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    TestImp ti = new TestImp();
    Executor executor = new SameThreadSubmitterExecutor();
    ch.addCallback(ti, executor);

    assertEquals(1, ch.registeredCallbackCount());
    assertTrue(ch.callbacks.get(ti) == executor);
  }
  
  @Test
  public void addCallbackFromCallTest() {
    int firstCallInt = 42;
    String firstCallStr = StringUtils.randomString(10);
    int secondCallInt = 1337;
    String secondCallStr = StringUtils.randomString(10);
    final TestImp addedCallback = new TestImp();
    final CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    TestImp ti = new TestImp() {
      @Override
      public void call(int i, String s) {
        super.call(i, s);
        ch.addCallback(addedCallback);
      }
    };
    ch.addCallback(ti);
    ch.addCallback(new TestImp());
    ch.addCallback(new TestImp());
    TestImp lastCallback = new TestImp();
    ch.addCallback(lastCallback);
    
    ch.call().call(firstCallInt, firstCallStr);
    // verify the other callbacks were called
    assertEquals(firstCallInt, lastCallback.lastInt);
    assertEquals(firstCallStr, lastCallback.lastString);

    assertEquals(5, ch.registeredCallbackCount());
    
    // verify new callback can be called
    ch.call().call(secondCallInt, secondCallStr);
    assertEquals(secondCallInt, addedCallback.lastInt);
    assertEquals(secondCallStr, addedCallback.lastString);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addCallbackFail() {
    CallbackHelper.build(TestInterface.class).addCallback(null);
  }
  
  @Test
  public void removeCallbackTest() {
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    TestImp ti = new TestImp();
    
    assertFalse(ch.removeCallback(null));
    
    ch.addCallback(ti);
    assertFalse(ch.removeCallback(null));
    assertFalse(ch.removeCallback(new TestImp()));
    assertEquals(1, ch.registeredCallbackCount());
    
    assertTrue(ch.removeCallback(ti));
    assertEquals(0, ch.registeredCallbackCount());
    
    ch.call().call(10, StringUtils.randomString(10));
    // verify not called
    assertNull(ti.lastString);
  }
  
  @Test
  public void removeCallbackFromCallTest() {
    int firstCallInt = 42;
    String firstCallStr = StringUtils.randomString(10);
    int secondCallInt = 1337;
    String secondCallStr = StringUtils.randomString(10);
    final TestImp removedCallback = new TestImp();
    final CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    TestImp ti = new TestImp() {
      @Override
      public void call(int i, String s) {
        super.call(i, s);
        ch.removeCallback(removedCallback);
      }
    };
    ch.addCallback(new TestImp());
    ch.addCallback(new TestImp());
    ch.addCallback(ti);
    ch.addCallback(new TestImp());
    ch.addCallback(new TestImp());
    ch.addCallback(removedCallback);
    ch.addCallback(new TestImp());
    ch.addCallback(new TestImp());
    TestImp lastCallback = new TestImp();
    ch.addCallback(lastCallback);
    
    ch.call().call(firstCallInt, firstCallStr);
    // verify the other callbacks were called
    assertEquals(firstCallInt, lastCallback.lastInt);
    assertEquals(firstCallStr, lastCallback.lastString);
    
    // verify callback was removed
    assertEquals(8, ch.registeredCallbackCount());
    
    // call again and verify it does not call removed callback
    ch.call().call(secondCallInt, secondCallStr);
    assertEquals(firstCallInt, removedCallback.lastInt);
    assertEquals(firstCallStr, removedCallback.lastString);
  }
  
  @Test
  public void registeredListenerCountTest() {
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    
    assertEquals(0, ch.registeredCallbackCount());
    
    ch.addCallback(new TestImp());
    assertEquals(1, ch.registeredCallbackCount());
  }
  
  @Test
  public void clearCallbacksTest() {
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    ch.addCallback(new TestImp());
    ch.addCallback(new TestImp());
    assertEquals(2, ch.registeredCallbackCount());
    
    ch.clearCallbacks();
    
    assertEquals(0, ch.registeredCallbackCount());
  }
  
  @Test
  public void callTest() {
    callTest(false);
  }
  
  @Test
  public void callWithExecutorTest() {
    callTest(true);
  }
  
  private static void callTest(boolean useExecutor) {
    int testInt = 10;
    String testStr = StringUtils.randomString(10);
    List<TestImp> callbacks = new ArrayList<TestImp>(TEST_QTY);
    CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
    
    for (int i = 0; i < TEST_QTY; i++) {
      TestImp ti = new TestImp();
      callbacks.add(ti);
      if (useExecutor) {
        ch.addCallback(ti, SameThreadSubmitterExecutor.instance());
      } else {
        ch.addCallback(ti);
      }
    }
    
    ch.call().call(testInt, testStr);
    
    Iterator<TestImp> it = callbacks.iterator();
    while (it.hasNext()) {
      TestImp ti = it.next();
      assertEquals(testInt, ti.lastInt);
      assertEquals(testStr, ti.lastString);
    }
  }
  
  @Test
  public void callMultipleFunctionCallbacksTest() {
    int testInt = 10;
    String testStr = StringUtils.randomString(10);
    List<TestMultipleFunctionImp> callbacks = new ArrayList<TestMultipleFunctionImp>(TEST_QTY);
    CallbackHelper<TestMultipleFunctionInterface> ch = CallbackHelper.build(TestMultipleFunctionInterface.class);
    
    for (int i = 0; i < TEST_QTY; i++) {
      TestMultipleFunctionImp ti = new TestMultipleFunctionImp();
      callbacks.add(ti);
      ch.addCallback(ti);
    }
    
    ch.call().call1(testInt);
    
    Iterator<TestMultipleFunctionImp> it = callbacks.iterator();
    while (it.hasNext()) {
      TestMultipleFunctionImp ti = it.next();
      assertEquals(testInt, ti.lastCall1Int);
      assertNull(ti.lastCall2String);
    }
    
    ch.call().call2(testStr);
    
    it = callbacks.iterator();
    while (it.hasNext()) {
      TestMultipleFunctionImp ti = it.next();
      assertEquals(testInt, ti.lastCall1Int);
      assertEquals(testStr, ti.lastCall2String);
    }
  }
  
  @Test
  public void callbackExceptionTest() {
    UncaughtExceptionHandler ueh = Thread.getDefaultUncaughtExceptionHandler();
    try {
      int testInt = 10;
      String testStr = StringUtils.randomString(10);
      TestUncaughtExceptionHandler testHandler = new TestUncaughtExceptionHandler();
      Thread.setDefaultUncaughtExceptionHandler(testHandler);
      final RuntimeException e = new RuntimeException();
      CallbackHelper<TestInterface> ch = CallbackHelper.build(TestInterface.class);
      ch.addCallback(new TestInterface() {
        @Override
        public void call(int i, String s) {
          throw e;
        }
      });
      TestImp ti = new TestImp();
      ch.addCallback(ti);

      ch.call().call(testInt, testStr);
      
      // verify exception was handled
      assertTrue(Thread.currentThread() == testHandler.getCalledWithThread());
      assertTrue(e == testHandler.getCalledWithThrowable());

      // verify other callbacks were called
      assertEquals(testInt, ti.lastInt);
      assertEquals(testStr, ti.lastString);
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(ueh);
    }
  }
  
  @Test (expected = RuntimeException.class)
  public void callFail() {
    @SuppressWarnings("rawtypes")
    CallbackHelper<List> ch = CallbackHelper.build(List.class);
    ch.call().get(0);
  }
  
  public interface TestInterface {
    public void call(int i, String s);
  }
  
  public interface TestMultipleFunctionInterface {
    public void call1(int i);
    public void call2(String s);
  }
  
  private static class TestImp implements TestInterface {
    private int lastInt = -1;
    private String lastString = null;
    
    @Override
    public void call(int i, String s) {
      lastInt = i;
      lastString = s;
    }
  }
  
  private static class TestMultipleFunctionImp implements TestMultipleFunctionInterface {
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
