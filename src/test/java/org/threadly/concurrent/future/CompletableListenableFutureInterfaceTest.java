package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.Clock;
import org.threadly.util.StackSuppressedRuntimeException;
import org.threadly.util.StringUtils;

@SuppressWarnings("javadoc")
public abstract class CompletableListenableFutureInterfaceTest extends ListenableFutureInterfaceTest {
  @Override
  protected ListenableFutureFactory makeListenableFutureFactory() {
    return makeCompletableListenableFutureFactory();
  }
  
  protected abstract CompletableListenableFutureFactory makeCompletableListenableFutureFactory();
  
  @Test
  public void completeWithResultTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.completeWithResult(null));
    assertFalse(slf.completeWithResult(null));
    assertFalse(slf.completeWithFailure(new StackSuppressedRuntimeException()));
  }
  
  @Test
  public void completeWithFailureTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.completeWithFailure(new StackSuppressedRuntimeException()));
    assertFalse(slf.completeWithFailure(new StackSuppressedRuntimeException()));
    assertFalse(slf.completeWithResult(null));
  }
  
  @Test
  public void cancelCompleteWithResultTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.cancel(false));
    assertFalse(slf.completeWithResult(null));
  }
  
  @Test
  public void cancelCompleteWithFailureTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.cancel(false));
    assertFalse(slf.completeWithFailure(null));
  }
  
  @Test
  public void isCompletedExceptionallyCompletedWithFailureTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertFalse(slf.isCompletedExceptionally());
    slf.completeWithFailure(new StackSuppressedRuntimeException());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Test
  public void isCompletedExceptionallyCompletedWithResultTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    slf.completeWithResult(null);
    assertFalse(slf.isCompletedExceptionally());
  }
  
  @Test
  public void isDoneAfterCancelTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.cancel(false));
    
    assertTrue(slf.isDone());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Test
  public void clearCompletedResultStateTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    slf.clearResult();
    
    assertTrue(slf.isDone());
    assertFalse(slf.isCancelled());
    assertFalse(slf.isCompletedExceptionally());
  }
  
  @Test
  public void clearCompletedFailureStateTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();
    
    assertTrue(slf.isDone());
    assertFalse(slf.isCancelled());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Test
  public void clearCompletedCanceledStateTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    slf.clearResult();
    
    assertTrue(slf.isDone());
    assertTrue(slf.isCancelled());
    assertTrue(slf.isCompletedExceptionally());
  }
  
  @Test
  public void clearCompletedResultFailureCallbackNoOpTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    slf.clearResult();
    
    slf.failureCallback((t) -> fail("Should not be invoked"));
    // no exception should throw, as state was known
  }
  
  @Test
  public void clearCompletedFailureResultCallbackNoOpTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();
    
    slf.resultCallback((r) -> fail("Should not be invoked"));
    // no exception should throw, as state was known
  }
  
  @Test
  public void clearCompletedCanceledResultCallbackNoOpTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    slf.clearResult();
    
    slf.resultCallback((r) -> fail("Should not be invoked"));
    // no exception should throw, as state was known
  }
  
  @Test
  public void clearCompletedResultCallbackTest() throws InterruptedException, TimeoutException {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    slf.clearResult();

    final AsyncVerifier av = new AsyncVerifier();
    slf.callback(new FutureCallback<Void>() {
      @Override
      public void handleResult(Void result) {
        av.fail("result provided");
      }

      @Override
      public void handleFailure(Throwable t) {
        av.assertTrue(t instanceof IllegalStateException);
        av.assertEquals("Result cleared", t.getMessage());
        av.signalComplete();
      }
    });
    av.waitForTest();
  }
  
  @Test
  public void clearCompletedResultFailureCallbackTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();

    AtomicReference<Throwable> ar = new AtomicReference<>();
    slf.failureCallback(ar::set);
    Throwable t = ar.get();
    assertTrue(t instanceof IllegalStateException);
    assertEquals("Result cleared", t.getMessage());
  }
  
  @Test
  public void clearCompletedResultGetFailureTest() throws InterruptedException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();

    Throwable t = slf.getFailure();
    assertTrue(t instanceof IllegalStateException);
    assertEquals("Result cleared, future get's not possible", t.getMessage());
  }
  
  @Test
  public void clearCompletedResultGetFailureTimeoutTest() throws InterruptedException, TimeoutException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();

    Throwable t = slf.getFailure(10, TimeUnit.MILLISECONDS);
    assertTrue(t instanceof IllegalStateException);
    assertEquals("Result cleared, future get's not possible", t.getMessage());
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearResultFail() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    slf.clearResult();
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearResultGetFail() throws InterruptedException, ExecutionException {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    
    slf.clearResult();
    slf.get();
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearFailureGetFail() throws InterruptedException, ExecutionException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    
    slf.clearResult();
    slf.get();
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearResultGetTimeoutFail() throws InterruptedException, ExecutionException, TimeoutException {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    
    slf.clearResult();
    slf.get(10, TimeUnit.MILLISECONDS);
    fail("Should have thrown exception");
  }
  
  @Test (expected = IllegalStateException.class)
  public void clearFailureGetTimeoutFail() throws InterruptedException, ExecutionException, TimeoutException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
            .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    
    slf.clearResult();
    slf.get(10, TimeUnit.MILLISECONDS);
    fail("Should have thrown exception");
  }
  
  @Test
  public void listenersCalledOnCompletedResultTest() {
    listenersCalledTest(false);
  }
  
  @Test
  public void listenersCalledOnCompletedFailureTest() {
    listenersCalledTest(true);
  }
  
  private void listenersCalledTest(boolean failure) {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    TestRunnable tr = new TestRunnable();
    slf.listener(tr);
    
    if (failure) {
      slf.completeWithFailure(new StackSuppressedRuntimeException());
    } else {
      slf.completeWithResult(null);
    }
    
    assertTrue(tr.ranOnce());
    
    // verify new additions also get called
    tr = new TestRunnable();
    slf.listener(tr);
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void callbackOnCompletedResultTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    String result = StringUtils.makeRandomString(5);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.completeWithResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void resultCallbackOnCompletedResultTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    String result = StringUtils.makeRandomString(5);
    TestFutureCallback tfc = new TestFutureCallback();
    slf.resultCallback(tfc::handleResult);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.completeWithResult(result);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void callbackOnCompletedFailureTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    Throwable failure = new StackSuppressedRuntimeException();
    TestFutureCallback tfc = new TestFutureCallback();
    slf.callback(tfc);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.completeWithFailure(failure);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void failureCallbackOnCompletedFailureTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    Throwable failure = new StackSuppressedRuntimeException();
    TestFutureCallback tfc = new TestFutureCallback();
    slf.failureCallback(tfc::handleFailure);
    
    assertEquals(0, tfc.getCallCount());
    
    slf.completeWithFailure(failure);
    
    assertEquals(1, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void cancelCompletableTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.cancel(false));
    assertFalse(slf.cancel(false));
    
    assertTrue(slf.isCancelled());
    assertTrue(slf.isDone());
  }
  
  @Test
  public void cancelCompletableRunsListenersTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    TestRunnable tr = new TestRunnable();
    slf.listener(tr);
    
    slf.cancel(false);
    
    assertTrue(tr.ranOnce());
  }
  
  @Test
  public void completeWithResultIsDoneTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertFalse(slf.isDone());
    
    slf.completeWithResult(null);

    assertTrue(slf.isDone());
  }
  
  @Test
  public void completeWithFailureIsDoneTest() {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertFalse(slf.isDone());
    
    slf.completeWithFailure(null);

    assertTrue(slf.isDone());
  }

  @Test (expected = TimeoutException.class)
  public void getTimeoutImmediateFail() throws InterruptedException, 
                                               ExecutionException, TimeoutException {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    slf.get(0, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }

  @Test
  public void getTimeoutFail() throws InterruptedException, 
                                      ExecutionException {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    long startTime = Clock.accurateForwardProgressingMillis();
    try {
      slf.get(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (TimeoutException e) {
      // expected
    }
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
  }

  @Test (expected = TimeoutException.class)
  public void getFailureTimeoutImmediateFail() throws InterruptedException, TimeoutException {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    slf.getFailure(0, TimeUnit.MILLISECONDS);
    fail("Exception should have thrown");
  }

  @Test
  public void getFailureTimeoutFail() throws InterruptedException {
    AbstractCompletableListenableFuture<String> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    long startTime = Clock.accurateForwardProgressingMillis();
    try {
      slf.getFailure(DELAY_TIME, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (TimeoutException e) {
      // expected
    }
    long endTime = Clock.accurateForwardProgressingMillis();
    
    assertTrue(endTime - startTime >= DELAY_TIME);
  }
  
  @Test (expected = ExecutionException.class)
  public void getNullExceptionTest() throws InterruptedException, 
                                            ExecutionException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeWithFailureCompletable(null);
    
    slf.get();
  }
  
  @Test
  public void getExecutionExceptionTest() throws InterruptedException {
    Exception failure = new Exception();
    AbstractCompletableListenableFuture<Object> slf = 
        makeCompletableListenableFutureFactory().makeWithFailureCompletable(failure);
    
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void getWithTimeoutExecutionExceptionTest() throws InterruptedException, 
                                                            TimeoutException {
    Exception failure = new StackSuppressedRuntimeException();
    AbstractCompletableListenableFuture<Object> slf = 
        makeCompletableListenableFutureFactory().makeWithFailureCompletable(failure);
    
    try {
      slf.get(100, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void getCancellationTest() throws InterruptedException, ExecutionException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    
    try {
      slf.get();
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
  }
  
  @Test
  public void getWithTimeoutCancellationTest() throws InterruptedException, ExecutionException, TimeoutException {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    
    try {
      slf.get(100, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
  }
  
  @Test
  public void cancelFlatMappedAsyncFutureTest() {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    AbstractCompletableListenableFuture<Void> asyncSLF = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    ListenableFuture<Void> mappedLF = slf.flatMap(asyncSLF);
      
    slf.completeWithResult(null);  // complete source future before cancel
    assertFalse(mappedLF.isDone());
    assertTrue(mappedLF.cancel(false)); // no interrupt needed, delegate future not started
    assertTrue(asyncSLF.isCancelled());
  }
  
  @Test
  public void flatMapReturnNullFail() throws InterruptedException {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    try {
      ListenableFuture<Void> mappedLF = slf.flatMap((o) -> null);
      slf.completeWithResult(null);
      mappedLF.get(); // retrieve error state set from mapper
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof NullPointerException);
      assertTrue(cause.getMessage().startsWith(InternalFutureUtils.NULL_FUTURE_MAP_RESULT_ERROR_PREFIX));
    }
  }
  
  @Test
  public void flatMapFailureReturnNullFail() throws InterruptedException {
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    try {
      ListenableFuture<?> mappedLF = slf.flatMapFailure(Exception.class, (o) -> null);
      slf.completeWithFailure(new Exception());
      mappedLF.get(); // retrieve error state set from mapper
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof NullPointerException);
      assertTrue(cause.getMessage().startsWith(InternalFutureUtils.NULL_FUTURE_MAP_RESULT_ERROR_PREFIX));
    }
  }
  
  @Test
  public void dontOptimizeListenerNotDoneTest() {
    TestableScheduler scheduler = new TestableScheduler();
    AbstractCompletableListenableFuture<Void> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    slf.listener(DoNothingRunnable.instance(), scheduler, ListenerOptimizationStrategy.InvokingThreadIfDone);
    
    slf.completeWithResult(null);  // we lied what thread it completes on
    
    assertEquals(1, scheduler.tick());  // one task for listener, despite scheduler match
  }
  
  @Test
  public void toStringNewTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeNewCompletable();
    
    assertTrue(slf.toString().contains("Not completed"));
  }
  
  @Test
  public void toStringResultTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    
    assertTrue(slf.toString().contains("Completed normally"));
  }
  
  @Test
  public void toStringFailureTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
          .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    
    String str = slf.toString();
    assertTrue(str.contains("Completed exceptionally"));
    assertTrue(str.contains(StackSuppressedRuntimeException.class.getSimpleName()));
  }
  
  @Test
  public void toStringCanceledTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    
    assertTrue(slf.toString().contains("Cancelled"));
  }
  
  @Test
  public void toStringResultClearedTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeWithResultCompletable(null);
    slf.clearResult();

    String str = slf.toString();
    assertTrue(str.contains("Completed normally"));
    assertTrue(str.contains("RESULT CLEARED"));
  }
  
  @Test
  public void toStringFailureClearedTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory()
          .makeWithFailureCompletable(new StackSuppressedRuntimeException());
    slf.clearResult();
    
    String str = slf.toString();
    assertTrue(str.contains("Completed exceptionally"));
    assertTrue(str.contains("RESULT CLEARED"));
  }
  
  @Test
  public void toStringCanceledClearedTest() {
    AbstractCompletableListenableFuture<?> slf = 
        makeCompletableListenableFutureFactory().makeCanceledCompletable();
    slf.clearResult();
    
    String str = slf.toString();
    assertTrue(str.contains("Cancelled"));
    assertTrue(str.contains("RESULT CLEARED"));
  }

  protected interface CompletableListenableFutureFactory extends ListenableFutureFactory {
    public <T> AbstractCompletableListenableFuture<T> makeNewCompletable();
    public AbstractCompletableListenableFuture<?> makeCanceledCompletable();
    public AbstractCompletableListenableFuture<Object> makeWithFailureCompletable(Exception e);
    public <T> AbstractCompletableListenableFuture<T> makeWithResultCompletable(T result);
    
    @Override
    default ListenableFuture<?> makeCanceled() {
      return makeCanceledCompletable();
    }
    
    @Override
    default ListenableFuture<Object> makeWithFailure(Exception e) {
      return makeWithFailureCompletable(e);
    }
    
    @Override
    default <T> ListenableFuture<T> makeWithResult(T result) {
      return makeWithResultCompletable(result);
    }
  }
}
