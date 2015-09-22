package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class AbstractFutureCallbackFailureHandlerTest {
  @Test
  public void resultIgnoredTest() {
    final AtomicReference<Throwable> failureProvided = new AtomicReference<Throwable>(null);
    new AbstractFutureCallbackFailureHandler() {
      @Override
      public void handleFailure(Throwable t) {
        failureProvided.set(t);
      }
    }.handleResult(new Exception());
    // no exception should throw or other weird behavior
    
    assertNull(failureProvided.get());
  }
  
  @Test
  public void failureProvidedTest() {
    final Throwable testFailure = new Exception();
    final AtomicReference<Throwable> failureProvided = new AtomicReference<Throwable>(null);
    new AbstractFutureCallbackFailureHandler() {
      @Override
      public void handleFailure(Throwable t) {
        failureProvided.set(t);
      }
    }.handleFailure(testFailure);
    
    assertTrue(testFailure == failureProvided.get());
  }
}
