package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class FutureCallbackResultHandlerTest {
  @Test
  public void failureIgnoredTest() {
    AtomicBoolean resultProvided = new AtomicBoolean(false);
    new FutureCallbackResultHandler<Object>((o) -> resultProvided.set(true))
        .handleFailure(new Exception());
    
    assertFalse(resultProvided.get());
  }
  
  @Test
  public void resultProvidedTest() {
    Object testResult = new Object();
    AtomicReference<Object> resultProvided = new AtomicReference<Object>(null);
    new FutureCallbackResultHandler<Object>((o) -> resultProvided.set(o))
        .handleResult(testResult);
    
    assertTrue(testResult == resultProvided.get());
  }
}
