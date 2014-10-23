package org.threadly.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintStream;
import java.io.OutputStream;
import java.util.StringTokenizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.threadly.concurrent.TestRuntimeFailureRunnable;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.util.ExceptionUtils.TransformedException;

@SuppressWarnings("javadoc")
public class ExceptionUtilsTest {
  @Before
  @After
  public void cleanup() {
    ExceptionUtils.setDefaultExceptionHandler(null);
    ExceptionUtils.setInheritableExceptionHandler(null);
    ExceptionUtils.setThreadExceptionHandler(null);
    Thread.setDefaultUncaughtExceptionHandler(null);
    Thread.currentThread().setUncaughtExceptionHandler(null);
  }
  
  @Test
  public void runRunnableTest() {
    TestRunnable tr = new TestRunnable();
    ExceptionUtils.runRunnable(tr);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void runRunnableThrownTest() {
    TestExceptionHandler exceptionHandler = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(exceptionHandler);
    RuntimeException failure = new RuntimeException();
    TestRuntimeFailureRunnable runnable = new TestRuntimeFailureRunnable(failure);
    
    ExceptionUtils.runRunnable(runnable);
    
    assertTrue(exceptionHandler.getLastThrowable() == failure);
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithoutUncaughtExceptionHandler() {
    // make sure no uncaughtExceptionHandler is set
    Thread.setDefaultUncaughtExceptionHandler(null);
    Thread.currentThread().setUncaughtExceptionHandler(null);
    
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
      
      assertTrue(sb.length() > 0);
      assertTrue(sb.toString().contains(ExceptionUtils.stackToString(e)));
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionThrowExceptionTest() {
    final RuntimeException thrownException = new RuntimeException();
    // set handler that will throw exception
    ExceptionUtils.setThreadExceptionHandler(new ExceptionHandlerInterface() {
      @Override
      public void handleException(Throwable thrown) {
        throw thrownException;
      }
    });
    
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
      
      assertTrue(sb.length() > 0);
      String resultStr = sb.toString();
      assertTrue(resultStr.contains(ExceptionUtils.stackToString(e)));
      assertTrue(resultStr.contains(ExceptionUtils.stackToString(thrownException)));
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithThreadExceptionHandler() {
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      TestExceptionHandler teh = new TestExceptionHandler();
      ExceptionUtils.setThreadExceptionHandler(teh);
      TestExceptionHandler uncalledHandler = new TestExceptionHandler();
      ExceptionUtils.setInheritableExceptionHandler(uncalledHandler);
      ExceptionUtils.setDefaultExceptionHandler(uncalledHandler);
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
        
      assertEquals(0, sb.length()); // should not have gone to std err
      assertEquals(0, uncalledHandler.getCallCount());
        
      assertEquals(1, teh.getCallCount());
      assertTrue(teh.getLastThrowable() == e);
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithInheritableThreadExceptionHandler() {
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      TestExceptionHandler teh = new TestExceptionHandler();
      ExceptionUtils.setInheritableExceptionHandler(teh);
      TestExceptionHandler uncalledHandler = new TestExceptionHandler();
      ExceptionUtils.setDefaultExceptionHandler(uncalledHandler);
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
        
      assertEquals(0, sb.length()); // should not have gone to std err
      assertEquals(0, uncalledHandler.getCallCount());
        
      assertEquals(1, teh.getCallCount());
      assertTrue(teh.getLastThrowable() == e);
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithDefaultThreadExceptionHandler() {
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      TestExceptionHandler teh = new TestExceptionHandler();
      ExceptionUtils.setDefaultExceptionHandler(teh);
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
        
      assertEquals(0, sb.length()); // should not have gone to std err
        
      assertEquals(1, teh.getCallCount());
      assertTrue(teh.getLastThrowable() == e);
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithDefaultUncaughtExceptionHandler() {
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
      Thread.setDefaultUncaughtExceptionHandler(ueh);
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
      
      assertEquals(0, sb.length()); // should not have gone to std err
      
      assertEquals(1, ueh.getCallCount());
      assertTrue(ueh.getCalledWithThread() == Thread.currentThread());
      assertTrue(ueh.getCalledWithThrowable() == e);
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @SuppressWarnings("resource")
  @Test
  public void handleExceptionWithUncaughtExceptionHandler() {
    PrintStream originalSystemErr = System.err;
    try {
      // set it up so System.err goes to a StringBuffer
      final StringBuilder sb = new StringBuilder();
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          sb.append((char)b);
        }
      }));
      
      TestUncaughtExceptionHandler ueh = new TestUncaughtExceptionHandler();
      Thread.currentThread().setUncaughtExceptionHandler(ueh);
      
      // make call
      Exception e = new Exception();
      ExceptionUtils.handleException(e);
      
      assertEquals(0, sb.length()); // should not have gone to std err
      
      assertEquals(1, ueh.getCallCount());
      assertTrue(ueh.getCalledWithThread() == Thread.currentThread());
      assertTrue(ueh.getCalledWithThrowable() == e);
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
  @Test
  public void handleExceptionWithNullException() {
    TestExceptionHandler teh = new TestExceptionHandler();
    
    ExceptionUtils.setDefaultExceptionHandler(teh);
    ExceptionUtils.handleException(null);
    
    // no exception should be thrown or called
    assertEquals(0, teh.getCallCount());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getRootCauseFail() {
    ExceptionUtils.getRootCause(null);
    fail("Exception should have thrown");
  }
  
  @Test
  public void getRootCauseNoCauseTest() {
    Exception e = new Exception();
    assertTrue(e == ExceptionUtils.getRootCause(e));
  }
  
  @Test
  public void getRootCauseTest() {
    Exception rootCause = new Exception();
    Exception e1 = new Exception(rootCause);
    Exception e2 = new Exception(e1);
    Exception e3 = new Exception(e2);
    
    
    assertTrue(ExceptionUtils.getRootCause(e3) == rootCause);
    assertTrue(ExceptionUtils.getRootCause(e2) == rootCause);
    assertTrue(ExceptionUtils.getRootCause(e1) == rootCause);
  }
  
  @Test
  public void makeRuntimeWithRuntimeTest() {
    RuntimeException testException = new RuntimeException();

    RuntimeException resultException = ExceptionUtils.makeRuntime(testException);
    assertNotNull(resultException);

    // we expect the exact same reference to come out
    assertTrue(testException == resultException);
  }

  @Test
  public void makeRuntimeWithNonRuntimeTest() {
    Exception testException = new Exception();

    RuntimeException resultException = ExceptionUtils.makeRuntime(testException);
    assertNotNull(resultException);
    assertTrue(resultException instanceof TransformedException);

    // verify stack trace does not contain Util.makeRuntime inside it for when it created a new exception
    StackTraceElement[] stack = resultException.getStackTrace();
    for (StackTraceElement ste : stack) {
      assertFalse(ste.getClass().getName().equals(ExceptionUtils.class.getName()));
    }

    // verify the cause was our original exception
    assertTrue(testException == resultException.getCause());
  }

  @Test
  public void makeRuntimeWithNullTest() {
    RuntimeException resultException = ExceptionUtils.makeRuntime(null);
    assertNotNull(resultException);

    // verify stack trace does not contain Util.makeRuntime inside it for when it created a new exception
    StackTraceElement[] stack = resultException.getStackTrace();
    for (StackTraceElement ste : stack) {
      assertFalse(ste.getClass().getName().equals(ExceptionUtils.class.getName()));
    }

    // verify that no cause is provided
    assertNull(resultException.getCause());
  }
  
  @Test
  public void stackToStringTest() {
    String message = "TestMessage";
    Exception testException = new Exception(message);
    String result = ExceptionUtils.stackToString(testException);
    assertNotNull(result);
    
    StringTokenizer st = new StringTokenizer(result, "\n");
    assertEquals(testException.getStackTrace().length + 1, st.countTokens());
    assertTrue(result.contains(message));
  }
  
  @Test
  public void stackToStringNullTest() {
    String result = ExceptionUtils.stackToString((Throwable)null);
    
    assertNotNull(result);
    
    assertEquals(0, result.length());
  }
  
  @Test
  public void writeStackToBuilderTest() {
    String message = "TestMessage";
    Exception testException = new Exception(message);
    StringBuilder sb = new StringBuilder();
    ExceptionUtils.writeStackTo(testException, sb);
    String result = sb.toString();
    assertNotNull(result);
    
    StringTokenizer st = new StringTokenizer(result, "\n");
    assertEquals(testException.getStackTrace().length + 1, st.countTokens());
    assertTrue(result.contains(message));
  }
  
  @Test
  public void writeStackToBuilderNullTest() {
    StringBuilder sb = new StringBuilder();
    ExceptionUtils.writeStackTo((Throwable)null, sb);
    
    assertEquals(0, sb.length());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void writeStackToBuilderFail() {
    ExceptionUtils.writeStackTo(new Exception(), (StringBuilder)null);
  }
  
  @Test
  public void writeStackToBufferTest() {
    String message = "TestMessage";
    Exception testException = new Exception(message);
    StringBuffer sb = new StringBuffer();
    ExceptionUtils.writeStackTo(testException, sb);
    String result = sb.toString();
    assertNotNull(result);
    
    StringTokenizer st = new StringTokenizer(result, "\n");
    assertEquals(testException.getStackTrace().length + 1, st.countTokens());
    assertTrue(result.contains(message));
  }
  
  @Test
  public void writeStackToBufferNullTest() {
    StringBuffer sb = new StringBuffer();
    ExceptionUtils.writeStackTo(null, sb);
    
    assertEquals(0, sb.length());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void writeStackToBufferFail() {
    ExceptionUtils.writeStackTo(new Exception(), (StringBuffer)null);
  }
  
  @Test
  public void stackToStringArrayTest() {
    Exception testException = new Exception();
    String result = ExceptionUtils.stackToString(testException.getStackTrace());
    assertNotNull(result);
    
    StringTokenizer st = new StringTokenizer(result, "\n");
    assertEquals(testException.getStackTrace().length, st.countTokens());
  }
  
  @Test
  public void stackToStringArrayNullStackTest() {
    String result = ExceptionUtils.stackToString((StackTraceElement[])null);
    
    assertNotNull(result);
    
    assertEquals(0, result.length());
  }
  
  @Test
  public void writeArrayStackToBuilderTest() {
    Exception testException = new Exception();
    StringBuilder sb = new StringBuilder();
    ExceptionUtils.writeStackTo(testException.getStackTrace(), sb);
    String result = sb.toString();
    assertNotNull(result);
    
    StringTokenizer st = new StringTokenizer(result, "\n");
    assertEquals(testException.getStackTrace().length, st.countTokens());
  }
  
  @Test
  public void writeArrayStackToBuilderNullTest() {
    StringBuilder sb = new StringBuilder();
    ExceptionUtils.writeStackTo((StackTraceElement[])null, sb);
    
    assertEquals(0, sb.length());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void writeArrayStackFail() {
    ExceptionUtils.writeStackTo(new Exception().getStackTrace(), null);
  }
}
