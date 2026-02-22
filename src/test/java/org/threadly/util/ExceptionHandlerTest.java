package org.threadly.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class ExceptionHandlerTest extends ThreadlyTester {
  private static final PrintStream ORIG_STD_ERR = System.err;
  
  private StringBuilder stdErrBuilder; 
  
  @BeforeEach
  @SuppressWarnings("resource")
  public void setup() {
    stdErrBuilder = new StringBuilder();
    System.setErr(new PrintStream(new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        stdErrBuilder.append((char)b);
      }
    }));
  }
  
  @AfterEach
  public void cleanup() {
    System.setErr(ORIG_STD_ERR);
    stdErrBuilder = null;
  }
  
  @Test
  public void defaultIgnoreHandlerTest() {
    ExceptionHandler.IGNORE_HANDLER.handleException(new Exception());
    // no action should occur
    assertEquals(0, stdErrBuilder.length());
  }
  
  @Test
  public void defaultPrintStacktraceHandlerTest() {
    Exception e = new Exception();
    ExceptionHandler.PRINT_STACKTRACE_HANDLER.handleException(e);
    assertTrue(stdErrBuilder.length() > 0);
    assertEquals(ExceptionUtils.stackToString(e), stdErrBuilder.toString());
  }
}
