package org.threadly.concurrent.future;

import static org.junit.Assert.*;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import org.threadly.ThreadlyTester;
import org.threadly.concurrent.CentralThreadlyPool;
import org.threadly.concurrent.DoNothingRunnable;
import org.threadly.concurrent.SameThreadSubmitterExecutor;
import org.threadly.concurrent.SingleThreadScheduler;
import org.threadly.concurrent.future.ListenableFuture.ListenerOptimizationStrategy;
import org.threadly.test.concurrent.AsyncVerifier;
import org.threadly.test.concurrent.BlockingTestRunnable;
import org.threadly.test.concurrent.TestCondition;
import org.threadly.test.concurrent.TestRunnable;
import org.threadly.test.concurrent.TestableScheduler;
import org.threadly.util.ExceptionUtils;
import org.threadly.util.StringUtils;
import org.threadly.util.StackSuppressedRuntimeException;
import org.threadly.util.TestExceptionHandler;

@SuppressWarnings("javadoc")
public abstract class ListenableFutureInterfaceTest extends ThreadlyTester {
  protected abstract ListenableFutureFactory makeListenableFutureFactory();
  
  @After
  public void cleanup() {
    ExceptionUtils.setThreadExceptionHandler(null);
  }
  
  @Test
  public void isDoneWithResultTest() {
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(null);
    
    assertTrue(lf.isDone());
    assertFalse(lf.isCompletedExceptionally());
    assertFalse(lf.isCancelled());
  }
  
  @Test
  public void isDoneWithFailureTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    
    assertTrue(lf.isDone());
    assertTrue(lf.isCompletedExceptionally());
    assertFalse(lf.isCancelled());
  }
  
  @Test
  public void isDoneCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    
    assertTrue(lf.isDone());
    assertTrue(lf.isCompletedExceptionally());
    assertTrue(lf.isCancelled());
  }
  
  @Test
  public void getResultTest() throws InterruptedException, ExecutionException, TimeoutException {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(result);
    assertTrue(result == lf.get());
    assertTrue(result == lf.get(1, TimeUnit.MILLISECONDS));
    assertNull(lf.getFailure());
    assertNull(lf.getFailure(1, TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void getFailureTest() throws InterruptedException, TimeoutException {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    try {
      lf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
    try {
      lf.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
    assertTrue(failure == lf.getFailure());
    assertTrue(failure == lf.getFailure(1, TimeUnit.MILLISECONDS));
  }
  
  @Test
  public void getCanceledTest() throws InterruptedException, TimeoutException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    try {
      lf.get();
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
    try {
      lf.get(1, TimeUnit.MILLISECONDS);
      fail("Exception should have thrown");
    } catch (CancellationException e) {
      // expected
    }
    assertTrue(lf.getFailure() instanceof CancellationException);
    assertTrue(lf.getFailure(1, TimeUnit.MILLISECONDS) instanceof CancellationException);
  }
  
  @Test
  public void callbackAlreadyDoneFutureTest() {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(result);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.callback(tfc);
    lf.callback(tfc, SameThreadSubmitterExecutor.instance());
    lf.callback(tfc, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.callback(tfc, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    assertEquals(4, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void resultCallbackAlreadyDoneFutureTest() {
    String result = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(result);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.resultCallback(tfc::handleResult);
    lf.resultCallback(tfc::handleResult, SameThreadSubmitterExecutor.instance());
    lf.resultCallback(tfc::handleResult, SameThreadSubmitterExecutor.instance(), 
                      ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.resultCallback(tfc::handleResult, SameThreadSubmitterExecutor.instance(), 
                      ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    assertEquals(4, tfc.getCallCount());
    assertTrue(result == tfc.getLastResult());
  }
  
  @Test
  public void callbackExecutionExceptionAlreadyDoneTest() {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.callback(tfc);
    lf.callback(tfc, SameThreadSubmitterExecutor.instance());
    lf.callback(tfc, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.callback(tfc, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    assertEquals(4, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void failureCallbackExecutionExceptionAlreadyDoneTest() {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    
    TestFutureCallback tfc = new TestFutureCallback();
    lf.failureCallback(tfc::handleFailure);
    lf.failureCallback(tfc::handleFailure, SameThreadSubmitterExecutor.instance());
    lf.failureCallback(tfc::handleFailure, SameThreadSubmitterExecutor.instance(), 
                       ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.failureCallback(tfc::handleFailure, SameThreadSubmitterExecutor.instance(), 
                       ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    assertEquals(4, tfc.getCallCount());
    assertTrue(failure == tfc.getLastFailure());
  }
  
  @Test
  public void listenerAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    
    TestRunnable tr = new TestRunnable();
    lf.listener(tr);
    lf.listener(tr, SameThreadSubmitterExecutor.instance());
    lf.listener(tr, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.listener(tr, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);

    assertEquals(4, tr.getRunCount());
  }
  
  @Test
  public void listenerExecutionExceptionAlreadyDoneTest() {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);

    TestRunnable tr = new TestRunnable();
    lf.listener(tr);
    lf.listener(tr, SameThreadSubmitterExecutor.instance());
    lf.listener(tr, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.listener(tr, SameThreadSubmitterExecutor.instance(), 
                ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);

    assertEquals(4, tr.getRunCount());
  }
  
  @Test
  public void mapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.map((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    });
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void mapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void mapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void mapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(teh);
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(1, teh.getCallCount());
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.map((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    }, scheduler);
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());  // not executed, instead error is detected and returned in thread

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void mapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.map((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void mapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(teh);
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.map((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(1, teh.getCallCount());
  }
  
  @Test
  public void cancelWhileMappedFunctionRunningInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean interrupted = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.map((o) -> {
        started.set(true);
        try {
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
        return null;
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertTrue(mappedLF.cancel(true));
      new TestCondition(() -> interrupted.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelWhileMappedFunctionRunningNoInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean completed = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.map((o) -> {
        started.set(true);
        try {
          Thread.sleep(DELAY_TIME * 10);
          completed.set(true);
        } catch (InterruptedException e) {
          // should not occur, if it does completed will never set
        }
        return null;
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertTrue(mappedLF.cancel(false));
      new TestCondition(() -> completed.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void throwMapBasicTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    Function<String, String> mapper = (s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    };
   
    assertTrue(translatedObject == lf.throwMap(mapper).get());
    assertTrue(translatedObject == lf.throwMap(mapper, SameThreadSubmitterExecutor.instance()).get());
    assertTrue(translatedObject == lf.throwMap(mapper, SameThreadSubmitterExecutor.instance(), null).get());
  }
  
  @Test
  public void throwMapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.throwMap((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    });
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void throwMapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void throwMapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void throwMapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(teh);
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(0, teh.getCallCount());
  }
  
  @Test
  public void throwMapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.throwMap((s) -> {
      if (s == sourceObject) {
        return translatedObject;
      } else {
        // test failure
        return null;
      }
    }, scheduler);
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void throwMapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());  // not executed, instead error is detected and returned in thread

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void throwMapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> {
      mapperRan.set(true);
      return null;
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void throwMapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestExceptionHandler teh = new TestExceptionHandler();
    ExceptionUtils.setThreadExceptionHandler(teh);
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.throwMap((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
    assertEquals(0, teh.getCallCount());
  }
  
  @Test
  public void flatMapAlreadyDoneTest() throws InterruptedException, ExecutionException {
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.flatMap((s) -> {
      if (s == sourceObject) {
        return FutureUtils.immediateResultFuture(translatedObject);
      } else {
        // test failure
        return FutureUtils.immediateResultFuture(null);
      }
    });
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void flatMapAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapAlreadyCanceledTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    });

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void flatMapAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> { throw failure; });

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapAlreadyDoneMapperReturnFailedFutureTest() throws InterruptedException {
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> FutureUtils.immediateFailureFuture(failure));

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapAlreadyDoneReturnNullFail() throws InterruptedException {
    try {
      makeListenableFutureFactory().makeWithResult(null)
        .flatMap((o) -> null).get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof NullPointerException);
      assertTrue(cause.getMessage().startsWith(InternalFutureUtils.NULL_FUTURE_MAP_RESULT_ERROR_PREFIX));
    }
  }
  
  @Test
  public void cancelWhileFlatMappedMapFunctionRunningInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean interrupted = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
        started.set(true);
        try {
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
        return FutureUtils.immediateResultFuture(null);
      }, sts);
      
      new TestCondition(() -> started.get()).blockTillTrue();
      assertTrue(mappedLF.cancel(true));
      new TestCondition(() -> interrupted.get()).blockTillTrue();
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelWhileFlatMappedMapFunctionRunningNoInterruptTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      AtomicBoolean started = new AtomicBoolean();
      BlockingTestRunnable btr = new BlockingTestRunnable();
      AtomicBoolean completed = new AtomicBoolean();
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
        started.set(true);
        btr.run(); // block till ready
        completed.set(true);
        return FutureUtils.immediateResultFuture(null);
      }, sts);
      
      try {
        new TestCondition(() -> started.get()).blockTillTrue();
        assertFalse(mappedLF.cancel(false));
        btr.unblock();
        new TestCondition(() -> completed.get()).blockTillTrue();
      } finally {
        btr.unblock();
      }
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void cancelFlatMappedCompletedFutureTest() {
    SingleThreadScheduler sts = new SingleThreadScheduler();
    try {
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
      ListenableFuture<Void> scheduledFuture = sts.submitScheduled(DoNothingRunnable.instance(), null, 10_000);
      ListenableFuture<Void> mappedLF = lf.flatMap((o) -> scheduledFuture);
      
      assertTrue(mappedLF.cancel(false)); // no interrupt needed, delegate future not started
      assertTrue(scheduledFuture.isCancelled());
    } finally {
      sts.shutdownNow();
    }
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    String sourceObject = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(sourceObject);
    String translatedObject = StringUtils.makeRandomString(10);
    ListenableFuture<String> mappedLF = lf.flatMap((s) -> {
      if (s == sourceObject) {
        return FutureUtils.immediateResultFuture(translatedObject);
      } else {
        // test failure
        return FutureUtils.immediateResultFuture(null);
      }
    }, scheduler);
    
    assertEquals(1, scheduler.tick());
    
    assertTrue(mappedLF.isDone());
    assertTrue(translatedObject == mappedLF.get());
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneExecutionExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    Exception failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(failure);
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    }, scheduler);
    
    assertEquals(0, scheduler.tick());  // failure state should be detected without execution

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapWithExecutorAlreadyCanceledTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeCanceled();
    AtomicBoolean mapperRan = new AtomicBoolean(false);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> {
      mapperRan.set(true);
      return FutureUtils.immediateResultFuture(null);
    }, scheduler);
    
    assertEquals(0, scheduler.tick());

    assertTrue(mappedLF.isDone());
    assertTrue(mappedLF.isCancelled());
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneMapperThrowExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> { throw failure; }, scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void flatMapWithExecutorAlreadyDoneMapperReturnFailedFutureTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException failure = new StackSuppressedRuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithResult(null);
    ListenableFuture<Void> mappedLF = lf.flatMap((o) -> FutureUtils.immediateFailureFuture(failure), scheduler);
  
    assertEquals(1, scheduler.tick());

    assertTrue(mappedLF.isDone());
    verifyFutureFailure(mappedLF, failure);
  }
  
  @Test
  public void mapFailureAlreadyDoneBasicTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    ListenableFuture<?> finalLF;
    
    finalLF = lf.mapFailure(Exception.class, (t) -> { throw finalException; });
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);

    finalLF = lf.mapFailure(Exception.class, (t) -> { throw finalException; }, 
                            SameThreadSubmitterExecutor.instance());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
    
    finalLF = lf.mapFailure(Exception.class, (t) -> { throw finalException; }, 
                            SameThreadSubmitterExecutor.instance(), null);
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void mapFailureAlreadyDoneIgnoredTest() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = lf.mapFailure(Throwable.class, 
                                                     (t) -> { mapped.set(true); return new Object(); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void mapFailureAlreadyDoneIgnoredFailureTypeTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(RuntimeException.class, 
                                                (t) -> { mapped.set(true); return null; });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void mapFailureAlreadyDoneIntoExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); throw finalException; });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void mapFailureAlreadyDoneIntoExceptionNullClassTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(null, 
                                                (t) -> { mapped.set(true); throw finalException; });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  private static void verifyFailureConditionFuture(ListenableFuture<?> finalLF, 
                                                   Exception expectedFailure) throws InterruptedException {
    assertTrue(finalLF.isDone());
    try {
      finalLF.get();
      fail("Exception should have thrown");
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() == expectedFailure);
    }
  }
  
  @Test
  public void mapFailureAlreadyDoneIntoResultTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); return null; });

    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    assertNull(finalLF.get());
  }
  
  @Test
  public void mapFailureAlreadyDoneIntoResultNullClassTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(null, (t) -> { mapped.set(true); return null; });

    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    assertNull(finalLF.get());
  }
  
  @Test
  public void flatMapFailureBasicTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    ListenableFuture<?> finalLF;
    
    finalLF = lf.flatMapFailure(Exception.class, (t) -> { throw finalException; });
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
    
    finalLF = lf.flatMapFailure(Exception.class, (t) -> { throw finalException; }, 
                                SameThreadSubmitterExecutor.instance());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
    
    finalLF = lf.flatMapFailure(Exception.class, (t) -> { throw finalException; }, 
                                SameThreadSubmitterExecutor.instance(), null);
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureAlreadyDoneIgnoredTest() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Throwable.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(new Object()); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void flatMapFailureAlreadyDoneIgnoredFailureTypeTest() {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(RuntimeException.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); });
    
    assertFalse(mapped.get());
    assertTrue(lf == finalLF);
  }
  
  @Test
  public void flatMapFailureAlreadyDoneIntoThrownExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.flatMapFailure(Exception.class, 
                                                    (t) -> { mapped.set(true); throw finalException; });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureAlreadyDoneIntoReturnedExceptionTest() throws InterruptedException {
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateFailureFuture(finalException); });

    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureAlreadyDoneIntoResultTest() throws InterruptedException, ExecutionException {
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); });

    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    assertNull(finalLF.get());
  }
  
  @Test
  public void flatMapFailureAlreadyDoneReturnNullFail() throws InterruptedException {
    try {
      ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
      lf.flatMapFailure(Exception.class, (t) -> null).get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof NullPointerException);
      assertTrue(cause.getMessage().startsWith(InternalFutureUtils.NULL_FUTURE_MAP_RESULT_ERROR_PREFIX));
    }
  }
  
  @Test
  public void mapFailureWithExecutorIgnoredTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = lf.mapFailure(Throwable.class, 
                                                     (t) -> { mapped.set(true); return new Object(); }, 
                                                     scheduler);

    assertTrue(finalLF == lf);  // optimized, no mapping, no new future needed
    assertEquals(0, scheduler.tick());
  }
  
  @Test
  public void mapFailureWithExecutorIgnoredFailureTypeTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(RuntimeException.class, 
                                                (t) -> { mapped.set(true); return null; }, 
                                                scheduler);

    assertTrue(finalLF == lf);  // optimized, no mapping, no new future needed
    assertEquals(0, scheduler.tick());
  }
  
  @Test
  public void mapFailureWithExecutorIntoExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); throw finalException; }, 
                                                scheduler);

    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertTrue(finalLF != lf);
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void mapFailureWithExecutorIntoResultTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.mapFailure(Exception.class, 
                                                (t) -> { mapped.set(true); return null; }, 
                                                scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertNull(finalLF.get());
  }
  
  @Test
  public void flatMapFailureWithExecutorIgnoredTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Throwable.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(new Object()); }, 
                          scheduler);

    assertTrue(finalLF == lf);
    assertEquals(0, scheduler.tick());
  }
  
  @Test
  public void flatMapFailureWithExecutorIgnoredFailureTypeTest() {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new Exception());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(RuntimeException.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); }, 
                          scheduler);

    assertTrue(finalLF == lf);
    assertEquals(0, scheduler.tick());
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoThrownExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = lf.flatMapFailure(Exception.class, 
                                                    (t) -> { mapped.set(true); throw finalException; }, 
                                                    scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoReturnedExceptionTest() throws InterruptedException {
    TestableScheduler scheduler = new TestableScheduler();
    RuntimeException finalException = new RuntimeException();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<Object> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateFailureFuture(finalException); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    verifyFailureConditionFuture(finalLF, finalException);
  }
  
  @Test
  public void flatMapFailureWithExecutorIntoResultTest() throws InterruptedException, ExecutionException {
    TestableScheduler scheduler = new TestableScheduler();
    ListenableFuture<?> lf = makeListenableFutureFactory().makeWithFailure(new StackSuppressedRuntimeException());
    AtomicBoolean mapped = new AtomicBoolean(false);
    ListenableFuture<?> finalLF = 
        lf.flatMapFailure(Exception.class, 
                          (t) -> { mapped.set(true); return FutureUtils.immediateResultFuture(null); }, 
                          scheduler);

    assertTrue(finalLF != lf);
    assertFalse(finalLF.isDone());
    assertEquals(1, scheduler.tick());
    assertTrue(finalLF.isDone());
    assertTrue(mapped.get());
    assertNull(finalLF.get());
  }

  @Test
  public void optimizeDoneListenerExecutorTest() throws InterruptedException, TimeoutException {
    optimizeDoneListenerExecutorTest(makeListenableFutureFactory().makeWithResult(null));
  }

  @Test
  public void dontOptimizeDoneListenerExecutorTest() throws InterruptedException, TimeoutException {
    dontOptimizeDoneListenerExecutorTest(makeListenableFutureFactory().makeWithResult(null));
  }
  
  public static void optimizeDoneListenerExecutorTest(ListenableFuture<?> lf) throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    Thread t = Thread.currentThread();
    Runnable listener = () -> {av.assertTrue(Thread.currentThread() == t) ; av.signalComplete();};
    
    lf.listener(listener, CentralThreadlyPool.computationPool(), 
                ListenerOptimizationStrategy.InvokingThreadIfDone);
    lf.listener(listener, CentralThreadlyPool.computationPool(), 
                ListenerOptimizationStrategy.SingleThreadIfExecutorMatchOrDone);
    
    av.waitForTest(10_000, 2);
  }
  
  public static void dontOptimizeDoneListenerExecutorTest(ListenableFuture<?> lf) throws InterruptedException, TimeoutException {
    AsyncVerifier av = new AsyncVerifier();
    Thread t = Thread.currentThread();
    Runnable threadTester = 
        () -> {av.assertFalse(Thread.currentThread() == t) ; av.signalComplete();};

    lf.listener(threadTester, CentralThreadlyPool.computationPool(), null);
    lf.listener(threadTester, CentralThreadlyPool.computationPool(), 
                ListenerOptimizationStrategy.None);
    lf.listener(threadTester, CentralThreadlyPool.computationPool(), 
                ListenerOptimizationStrategy.SingleThreadIfExecutorMatch);
    
    av.waitForTest(10_000, 3);
  }
  
  public static void mapStackDepthTest(ListenableFuture<Object> future, 
                                       Runnable futureCompleteTask, 
                                       int incompleteExpectedStackDepth, 
                                       int completedExpectedStackDepth) throws InterruptedException, TimeoutException {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      AsyncVerifier av1 = new AsyncVerifier();
      ListenableFuture<Object> mappedFuture = future;
      for (int i = 0; i < 10; i++) {
        if (i == 0) {
          mappedFuture = mappedFuture.map((ignored) -> null, SameThreadSubmitterExecutor.instance(), 
                                          ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatch);
        } else if (i == 1) {
          mappedFuture = mappedFuture.map((ignored) -> null, SameThreadSubmitterExecutor.instance());
        } else {
          mappedFuture = mappedFuture.map((ignored) -> null);
        }
      }
      mappedFuture.listener(() -> {
        int stackDepth = Thread.currentThread().getStackTrace().length;
        av1.assertEquals(incompleteExpectedStackDepth, stackDepth);
        av1.signalComplete();
      });
      
      scheduler.execute(futureCompleteTask);
      
      av1.waitForTest();
      
      // now verify already completed stack depth
      AsyncVerifier av2 = new AsyncVerifier();
      scheduler.execute(() -> {
        AtomicInteger mapCount = new AtomicInteger();
        AtomicReference<ListenableFuture<Object>> currFuture = new AtomicReference<>(future);
        Function<Object, Object> recurrsiveMapper = new Function<Object, Object>() {
          @Override
          public Object apply(Object arg0) {
            if (mapCount.incrementAndGet() < 10) {
              currFuture.set(currFuture.get().map(this));
            } else {
              av2.assertEquals(completedExpectedStackDepth, Thread.currentThread().getStackTrace().length);
              av2.signalComplete();
            }
            return null;
          }
        };
        future.map(recurrsiveMapper);
      });
      av2.waitForTest();
    } finally {
      scheduler.shutdown();
    }
  }
  
  public static void mapFailureStackDepthTest(ListenableFuture<Object> future, 
                                              Runnable futureCompleteTask, 
                                              int expectedStackDepth) throws InterruptedException, TimeoutException {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      AsyncVerifier av = new AsyncVerifier();
      ListenableFuture<Object> mappedFuture = future;
      for (int i = 0; i < 10; i++) {
        if (i == 0) {
          mappedFuture = mappedFuture.mapFailure(RuntimeException.class, (e) -> { throw e; }, 
                                                 SameThreadSubmitterExecutor.instance(), 
                                                 ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatch);
        } else if (i == 1) {
          mappedFuture = mappedFuture.mapFailure(RuntimeException.class, (e) -> { throw e; }, 
                                                 SameThreadSubmitterExecutor.instance());
        } else {
          mappedFuture = mappedFuture.mapFailure(RuntimeException.class, (e) -> { throw e; });
        }
      }
      mappedFuture.listener(() -> {
        int stackDepth = Thread.currentThread().getStackTrace().length;
        av.assertEquals(expectedStackDepth, stackDepth);
        av.signalComplete();
      });
      
      scheduler.execute(futureCompleteTask);
      
      av.waitForTest();
    } finally {
      scheduler.shutdown();
    }
  }
  
  public static void flatMapStackDepthTest(ListenableFuture<Object> future, 
                                           Runnable futureCompleteTask, 
                                           int incompleteExpectedStackDepth, 
                                           int completedExpectedStackDepth) throws InterruptedException, TimeoutException {
    SingleThreadScheduler scheduler = new SingleThreadScheduler();
    try {
      AsyncVerifier av1 = new AsyncVerifier();
      ListenableFuture<Object> mappedFuture = future;
      for (int i = 0; i < 10; i++) {
        if (i == 0) {
          mappedFuture = mappedFuture.flatMap((ignored) -> FutureUtils.immediateResultFuture(null), 
                                              SameThreadSubmitterExecutor.instance(), 
                                              ListenableFuture.ListenerOptimizationStrategy.SingleThreadIfExecutorMatch);
        } else if (i == 1) {
          mappedFuture = mappedFuture.flatMap((ignored) -> FutureUtils.immediateResultFuture(null), 
                                              SameThreadSubmitterExecutor.instance());
        } else {
          mappedFuture = mappedFuture.flatMap((ignored) -> FutureUtils.immediateResultFuture(null));
        }
      }
      mappedFuture.listener(() -> {
        int stackDepth = Thread.currentThread().getStackTrace().length;
        av1.assertEquals(incompleteExpectedStackDepth, stackDepth);
        av1.signalComplete();
      });
      
      scheduler.execute(futureCompleteTask);
      
      av1.waitForTest();
      
      // now verify already completed stack depth
      AsyncVerifier av2 = new AsyncVerifier();
      scheduler.execute(() -> {
        AtomicInteger mapCount = new AtomicInteger();
        AtomicReference<ListenableFuture<Object>> currFuture = new AtomicReference<>(future);
        Function<Object, ListenableFuture<Object>> recurrsiveMapper = new Function<Object, ListenableFuture<Object>>() {
          @Override
          public ListenableFuture<Object> apply(Object arg0) {
            if (mapCount.incrementAndGet() < 2) {
              currFuture.set(currFuture.get().flatMap(this));
            } else {
              av2.assertEquals(completedExpectedStackDepth, Thread.currentThread().getStackTrace().length);
              av2.signalComplete();
            }
            return FutureUtils.immediateResultFuture(null);
          }
        };
        future.flatMap(recurrsiveMapper);
      });
      av2.waitForTest();
    } finally {
      scheduler.shutdown();
    }
  }
  
  private static void verifyFutureFailure(ListenableFuture<?> f, Exception failure) throws InterruptedException {
    try {
      f.get();
      fail("Exception should have thrown");
    } catch (ExecutionException e) {
      assertTrue(failure == e.getCause());
    }
  }
  
  @Test
  public void asNullTest() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    
    assertNull(lf.as(__ -> null));
  }
  
  @Test
  public void asCompletableFutureWithResultTest() throws InterruptedException, ExecutionException {
    String str = StringUtils.makeRandomString(5);
    ListenableFuture<String> lf = makeListenableFutureFactory().makeWithResult(str);
    
    CompletableFuture<String> cf = lf.as(CompletableFutureAdapter::toCompletable);
    
    assertNotNull(cf);
    assertTrue(str == cf.get());
  }
  
  @Test
  public void asCompletableFutureWithFailureTest() throws InterruptedException {
    Exception e = new StackSuppressedRuntimeException();
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithFailure(e);
    
    CompletableFuture<Object> cf = lf.as(CompletableFutureAdapter::toCompletable);
    
    assertNotNull(cf);
    try {
      cf.get();
      fail("Exception should have thrown");
    } catch (ExecutionException ee) {
      assertTrue(ee.getCause() == e);
    }
  }
  
  @Test
  public void asSelfTest() {
    ListenableFuture<Integer> lf = makeListenableFutureFactory().makeWithResult(1);
    
    ListenableFuture<? extends Number> nlf = lf.as(f -> f);
    
    assertNotNull(nlf);
    assertTrue(nlf == lf);
  }
  
  @Test (expected = NullPointerException.class)
  public void asNullFunctionFailure() {
    ListenableFuture<Object> lf = makeListenableFutureFactory().makeWithResult(null);
    
    lf.as(null);
    fail("Exception should have thrown");
  }
  
  protected interface ListenableFutureFactory {
    public ListenableFuture<?> makeCanceled();
    public ListenableFuture<Object> makeWithFailure(Exception e);
    public <T> ListenableFuture<T> makeWithResult(T result);
  }
}
