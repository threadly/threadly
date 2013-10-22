package org.threadly.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintStream;
import java.io.OutputStream;
import java.util.StringTokenizer;

import org.junit.Test;
import org.threadly.concurrent.TestUncaughtExceptionHandler;
import org.threadly.util.ExceptionUtils.TransformedException;

@SuppressWarnings("javadoc")
public class ExceptionUtilsTest {
  @Test
  public void handleExceptionWithoutUncaughtExceptionHandler() {
    // make sure no uncaughtExceptionHandler is set
    Thread.setDefaultUncaughtExceptionHandler(null);
    
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
      
      assertTrue(sb.toString().equals(ExceptionUtils.stackToString(e)));
    } finally {
      System.setErr(originalSystemErr);
    }
  }
  
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
    String result = ExceptionUtils.stackToString(null);
    
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
    ExceptionUtils.writeStackTo(null, sb);
    
    assertEquals(0, sb.length());
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
}
