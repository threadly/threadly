package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings({"javadoc", "deprecation"})
public class FutureCallbackExceptionHandlerAdapterTest extends ThreadlyTester {
  @SuppressWarnings("unused")
  @Test (expected = IllegalArgumentException.class)
  public void constructorFail() {
    new FutureCallbackExceptionHandlerAdapter(null);
    fail("Exception should have thrown");
  }
  
  public void deligateCalledTest() {
    Throwable failure = new Exception();
    TestExceptionHandler handler = new TestExceptionHandler();
    FutureCallback<Object> adapter = new FutureCallbackExceptionHandlerAdapter(handler);
    adapter.handleResult(new Object()); // ignored
    adapter.handleFailure(failure);
    
    assertTrue(failure == handler.getLastThrowable());
  }
}
