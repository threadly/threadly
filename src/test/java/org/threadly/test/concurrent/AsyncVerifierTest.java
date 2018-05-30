package org.threadly.test.concurrent;

import static org.junit.Assert.*;

import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class AsyncVerifierTest extends ThreadlyTester {
  @BeforeClass
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  @AfterClass
  public static void cleanupClass() {
    Thread.setDefaultUncaughtExceptionHandler(null);
  }
  
  private static final int ASYNC_WAIT_AMOUNT = 2;
  
  private AsyncVerifier verifier;
  
  @Before
  public void setup() {
    verifier = new AsyncVerifier();
  }
  
  @After
  public void cleanup() {
    verifier = null;
  }
  
  @Test (expected = TimeoutException.class)
  public void waitForTestTimeout() throws InterruptedException, TimeoutException {
    verifier.waitForTest(1);
  }
  
  @Test
  public void signalCompleteBeforeWaitTest() throws InterruptedException, TimeoutException {
    verifier.signalComplete();
    
    verifier.waitForTest(1);  // no exception should throw as no blocking is needed
  }
  
  @Test
  public void signalCompleteAnotherThreadTest() {
    TestRunnable waitRunnable = new TestRunnable() {
      @Override
      public void handleRunFinish() {
        try {
          verifier.waitForTest();
        } catch (Exception e) {
          throw ExceptionUtils.makeRuntime(e);
        }
      }
    };
    new Thread(waitRunnable).start();
    
    // should unblock thread
    verifier.signalComplete();
    
    waitRunnable.blockTillFinished(); // should return quickly
  }
  
  @Test
  public void assertTrueTest() {
    verifier.assertTrue(true);
  }
  
  @Test (expected = RuntimeException.class)
  public void assertTrueFail() {
    verifier.assertTrue(false);
  }
  
  @Test
  public void assertFalseTest() {
    verifier.assertFalse(false);
  }
  
  @Test (expected = RuntimeException.class)
  public void assertFalseFail() {
    verifier.assertFalse(true);
  }
  
  @Test
  public void assertNullTest() {
    verifier.assertNull(null);
  }
  
  @Test (expected = RuntimeException.class)
  public void assertNullFail() {
    verifier.assertNull(new Object());
  }
  
  @Test
  public void assertNotNullTest() {
    verifier.assertNotNull(new Object());
  }
  
  @Test (expected = RuntimeException.class)
  public void assertNotNullFail() {
    verifier.assertNotNull(null);
  }
  
  @Test
  public void assertEqualsTest() {
    Object o = new Object();
    verifier.assertEquals(o, o);
    verifier.assertEquals(null, null);
    verifier.assertEquals(1, 1);
    verifier.assertEquals(1L, 1L);
    verifier.assertEquals(1.1, 1.1);
  }
  
  @Test (expected = RuntimeException.class)
  public void assertEqualsObjectFail() {
    verifier.assertEquals(null, new Object());
  }
  
  @Test (expected = RuntimeException.class)
  public void assertEqualsNumberFail() {
    verifier.assertEquals(1, 10.0);
  }
  
  @Test (expected = RuntimeException.class)
  public void failTest() {
    verifier.fail();
  }
  
  @Test
  public void failMsgTest() {
    String msg = StringUtils.makeRandomString(5);
    try {
      verifier.fail(msg);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      assertEquals(msg, e.getMessage());
    }
  }
  
  @Test
  public void failThrowableTest() {
    Exception failure = new Exception();
    try {
      verifier.fail(failure);
      fail("Exception should have thrown");
    } catch (RuntimeException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test (expected = RuntimeException.class)
  public void assertTrueFailAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.assertTrue(false);
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void assertFalseFailAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.assertFalse(true);
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void assertNullFailAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.assertNull(new Object());
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void assertNotNullFailAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.assertNotNull(null);
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void assertEqualsObjectFailAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.assertEquals(new Object(), new Object());
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void failTestAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.fail();
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void failMsgTestAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.fail("foo");
      }
    }).start();
    
    verifier.waitForTest();
  }
  
  @Test (expected = RuntimeException.class)
  public void failThrowableTestAnotherThreadTest() throws InterruptedException, TimeoutException {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(ASYNC_WAIT_AMOUNT);
        } catch (InterruptedException e) {
          // let thread exit
          return;
        }
        
        verifier.fail(new Exception());
      }
    }).start();
    
    verifier.waitForTest();
  }
}
