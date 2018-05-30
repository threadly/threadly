package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.threadly.ThreadlyTester;

@SuppressWarnings("javadoc")
public class FutureCallbackResultHandlerTest extends ThreadlyTester {
  @Test
  public void failureIgnoredTest() {
    AtomicBoolean resultProvided = new AtomicBoolean(false);
    new FutureCallbackResultHandler<>((o) -> resultProvided.set(true))
        .handleFailure(new Exception());
    
    assertFalse(resultProvided.get());
  }
  
  @Test
  public void resultProvidedTest() {
    Object testResult = new Object();
    AtomicReference<Object> resultProvided = new AtomicReference<>(null);
    new FutureCallbackResultHandler<>((o) -> resultProvided.set(o))
        .handleResult(testResult);
    
    assertTrue(testResult == resultProvided.get());
  }
}
