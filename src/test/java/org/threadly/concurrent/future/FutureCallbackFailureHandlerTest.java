package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class FutureCallbackFailureHandlerTest {
  @Test
  public void resultIgnoredTest() {
    AtomicReference<Throwable> failureProvided = new AtomicReference<>(null);
    new FutureCallbackFailureHandler((t) -> failureProvided.set(t)).handleResult(new Exception());
    // no exception should throw or other weird behavior
    
    assertNull(failureProvided.get());
  }
  
  @Test
  public void failureProvidedTest() {
    Throwable testFailure = new Exception();
    AtomicReference<Throwable> failureProvided = new AtomicReference<>(null);
    new FutureCallbackFailureHandler((t) -> failureProvided.set(t)).handleFailure(testFailure);
    
    assertTrue(testFailure == failureProvided.get());
  }
}
