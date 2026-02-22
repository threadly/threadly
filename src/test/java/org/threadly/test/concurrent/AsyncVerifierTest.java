package org.threadly.test.concurrent;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public class AsyncVerifierTest extends ThreadlyTester {
  @BeforeAll
  public static void setupClass() {
    setIgnoreExceptionHandler();
  }
  
  @AfterAll
  public static void cleanupClass() {
    Thread.setDefaultUncaughtExceptionHandler(null);
  }
  
  private static final int ASYNC_WAIT_AMOUNT = 2;
  
  private AsyncVerifier verifier;
  
  @BeforeEach
  public void setup() {
    verifier = new AsyncVerifier();
  }
  
  @AfterEach
  public void cleanup() {
    verifier = null;
  }
  
  @Test
  public void waitForTestTimeout(){
      assertThrows(TimeoutException.class, () -> {
      verifier.waitForTest(1);
      });
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
  
  @Test
  public void assertTrueFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertTrue(false);
      });
  }
  
  @Test
  public void assertFalseTest() {
    verifier.assertFalse(false);
  }
  
  @Test
  public void assertFalseFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertFalse(true);
      });
  }
  
  @Test
  public void assertNullTest() {
    verifier.assertNull(null);
  }
  
  @Test
  public void assertNullFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertNull(new Object());
      });
  }
  
  @Test
  public void assertNotNullTest() {
    verifier.assertNotNull(new Object());
  }
  
  @Test
  public void assertNotNullFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertNotNull(null);
      });
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
  
  @Test
  public void assertEqualsObjectFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertEquals(null, new Object());
      });
  }
  
  @Test
  public void assertEqualsNumberFail() {
      assertThrows(RuntimeException.class, () -> {
      verifier.assertEquals(1, 10.0);
      });
  }
  
  @Test
  public void failTest() {
      assertThrows(RuntimeException.class, () -> {
      verifier.fail();
      });
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
  
  @Test
  public void assertTrueFailAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void assertFalseFailAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void assertNullFailAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void assertNotNullFailAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void assertEqualsObjectFailAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void failTestAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void failMsgTestAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
  
  @Test
  public void failThrowableTestAnotherThreadTest(){
      assertThrows(RuntimeException.class, () -> {
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
      });
  }
}
